#!/usr/bin/env python3
"""
实测：对账流水线怎么发现四类差异、怎么分级处理、怎么证明修好了。

对应文章：GYD 第 3 篇《跨行清算对账》第四节（差异分类）、第五节（分级处理）、
第六节（冲正）、第八节（闭环验收）。

要回答的问题：
    正常路径下三本账总是平的，读者跑完 demo 什么差异都看不到。
    那文章讲的漏记、重复入账、金额不符、待查，到底长什么样？
    哪些能自动修好、哪些只能挂起等人？「修好了」又是怎么证明的？

依赖：
    仅标准库（urllib）。服务用 ./start.sh 起。

运行：
    cd c03-reconciliation
    ./start.sh                            # 起 postgres + rabbitmq + 三个服务
    python3 scripts/recon-experiment.py    # 依次跑六个场景

    单跑某一场景：
    python3 scripts/recon-experiment.py --only missing
    可选场景：baseline missing duplicate amount pending adjudicate

六个场景：
    1. baseline    正常转账 → 对账 → 三本账全平（对照组）
    2. missing     建行漏记 → 对账自动补记 → 闭环验收通过
    3. duplicate   建行重复入账 → 对账只标记、不自动冲正（判断不出冲哪组）
    4. amount      工行金额记错 → 对账挂起等人工（账仍是平的，试算平衡发现不了）
    5. pending     结算侧改回 PENDING → 对账两侧都判待查、不做任何修正
    6. adjudicate  人工裁决冲正掉多余那组 → 重新对账 → 差异消失

实测结果见运行输出。每一段都会打印对账 Job 的真实日志行（从结算服务日志里抓），
便于直接核对文章里的差异分类表与决策树。
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(HERE)

SETTLEMENT = "http://localhost:18080"
ICBC = "http://localhost:18081"   # 工行，文章场景里的付款行
CCB = "http://localhost:18082"    # 建行，文章场景里的收款行

SETTLEMENT_LOG = os.path.join(MODULE_DIR, "logs", "settlement.log")

# 文章场景：A（工行客户）转 10,000 元给 B（建行客户）
AMOUNT_CENTS = 1_000_000


# ---------------------------------------------------------------------------
# HTTP 工具
# ---------------------------------------------------------------------------

def post(url, body=None):
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode()
    return json.loads(raw) if raw else {}


def get(url):
    with urllib.request.urlopen(url, timeout=30) as resp:
        raw = resp.read().decode()
    return json.loads(raw) if raw else []


def settle(from_bank="ICBC", to_bank="CCB", amount_cents=AMOUNT_CENTS):
    """发一笔跨行转账，返回 tradeId"""
    return post(f"{SETTLEMENT}/api/settle", {
        "fromBank": from_bank, "toBank": to_bank, "amountCents": amount_cents,
    })["tradeId"]


def run_recon():
    """手动触发一轮对账（不等 60 秒定时任务）"""
    post(f"{SETTLEMENT}/api/recon/run")


def reset_environment():
    """清空三本账，让本轮实验从零开始。

    对账每轮都拉「当日全部」数据，不复位的话上一轮注入的差异会一直留在账上，
    简报里的笔数一路涨、每个场景的输出都混着别的场景的差异，没法单独看。
    """
    print("复位环境：清空结算记录与两家银行的账本...")
    for base, name in ((SETTLEMENT, "结算"), (ICBC, "工行"), (CCB, "建行")):
        endpoint = "/api/settlement/fault/reset" if base is SETTLEMENT else "/api/bank/fault/reset"
        try:
            post(base + endpoint)
        except Exception as exc:
            print(f"  {name} 复位失败：{type(exc).__name__}: {exc}")
            print("  如果是 404，说明服务端还是旧构建，重新 ./gradlew bootJar 后再跑。")
            sys.exit(1)
    print("  三本账已清空\n")


def settled_at(trade_id):
    """查结算记录里的 settledAt，注入故障时要原样带上"""
    rec = get(f"{SETTLEMENT}/api/settlement/{trade_id}")
    return rec["settledAt"]


def journal(base_url, trade_id):
    """拉某家银行的完整账本轨迹（原始分录 + 冲正分录）"""
    return get(f"{base_url}/api/bank/journal/{trade_id}")


def wait_booked(trade_id, base_url=CCB, expect=2, timeout=15):
    """等消费者把分录入账（MQ 是异步的，注入前必须确认正常记账已落库）"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(journal(base_url, trade_id)) >= expect:
            return True
        time.sleep(0.5)
    return False


# ---------------------------------------------------------------------------
# 日志抓取：把对账 Job 的真实输出拿出来对照
# ---------------------------------------------------------------------------

def log_size():
    try:
        return os.path.getsize(SETTLEMENT_LOG)
    except OSError:
        return 0


def log_since(offset):
    """读取 offset 之后新增的日志，只保留「本次手动触发」那一轮的对账行。

    定时对账（默认 60 秒一轮）可能在手动触发的前后自己跑一轮，日志里就会出现两段
    几乎一样的输出，读者分不清哪段是本场景的结果。这里按「开始日终对账」分轮，
    只取紧跟在「手动触发」之后的那一轮：从「手动触发」行起，到再下一次「开始日终
    对账」之前止（手动轮自己也有一行「开始日终对账」，要跳过它取下一个）。
    """
    try:
        with open(SETTLEMENT_LOG, "rb") as f:
            f.seek(offset)
            chunk = f.read().decode("utf-8", errors="replace")
    except OSError:
        return []

    raw = chunk.splitlines()
    trigger = next((i for i, l in enumerate(raw) if "手动触发一轮对账" in l), None)
    if trigger is None:
        segment = raw                      # 兜底：没找到手动触发行就全要
    else:
        starts = [i for i, l in enumerate(raw) if "开始日终对账" in l and i > trigger]
        end = starts[1] if len(starts) >= 2 else len(raw)
        segment = raw[trigger:end]

    keep = []
    for line in segment:
        if re.search(r"\[recon\]|闭环", line):
            # 去掉时间戳和线程名，只留可读部分
            m = re.search(r"(\[recon\].*)$", line)
            keep.append(m.group(1) if m else line.strip())
    return keep


def show_round(lines, trade_id, indent="    "):
    """打印一轮对账的日志：只展开本场景这笔交易的差异，其余折叠成一行。

    为什么需要折叠：挂起等人工裁决的差异不会自己消失，对账每一轮都会重新报一遍。
    跑到场景 5 时，日志里会同时出现场景 3 的重复入账和场景 4 的金额不符——
    这个「重复报告」本身是真实行为，但会把本场景要看的东西盖住。
    所以这里只展开当前 tradeId 的差异行，历史的折叠成一行计数。
    """
    if not lines:
        print(f"{indent}（本轮无对账日志输出）")
        return

    current, stale_count = [], 0
    for line in lines:
        is_diff = "[recon][P" in line or "候选分录" in line or "人工裁决命令示例" in line
        if is_diff and trade_id not in line:
            stale_count += 1
        else:
            current.append(line)

    for line in current:
        print(f"{indent}{line}")
    if stale_count:
        print(f"{indent}（另有 {stale_count} 行是前几个场景遗留的未处理差异，"
              f"每轮都会重报——挂起等人工的差异不会自己消失）")


# ---------------------------------------------------------------------------
# 账本打印：对应文章第六节「原始分录和冲正分录并排看」那张表
# ---------------------------------------------------------------------------

def print_journal(label, base_url, trade_id):
    entries = journal(base_url, trade_id)
    print(f"  {label}账本（tradeId={trade_id}，共 {len(entries)} 条）：")
    if not entries:
        print("    （空 —— 本行没有任何分录）")
        return entries
    direction_label = {"DEBIT": "借", "CREDIT": "贷"}
    account_label = {
        "customer_deposit": "客户存款",
        "pboc_settlement": "存放央行款项",
    }
    for e in entries:
        kind = "冲正" if e.get("reversalOf") else "原始"
        print("    [{}] {} {} ¥{:>10,.2f}  entryId={}{}".format(
            kind,
            direction_label.get(e["direction"], e["direction"]),
            account_label.get(e["accountId"], e["accountId"]),
            e["amountCents"] / 100,
            e["entryId"],
            f"  reversalOf={e['reversalOf']}" if e.get("reversalOf") else "",
        ))
    return entries


def net_by_account(entries):
    """算每个科目的「净分录」合计 —— 已被冲正的原始分录不计入。

    对账匹配用的就是这个口径（服务端 findNetByBookedAtBetween），
    在这里按同一规则复算一遍，就能拿账本数据直接证明「冲正之后钱数对了」。
    """
    reversed_ids = {e["reversalOf"] for e in entries if e.get("reversalOf")}
    net = {}
    for e in entries:
        if e["entryId"] in reversed_ids or e.get("reversalOf"):
            continue
        net[e["accountId"]] = net.get(e["accountId"], 0) + e["amountCents"]
    return net


def print_net(label, entries, indent="    "):
    """打印净分录合计，用于冲正前后对比"""
    net = net_by_account(entries)
    account_label = {"customer_deposit": "客户存款", "pboc_settlement": "存放央行款项"}
    parts = [f"{account_label.get(k, k)} ¥{v / 100:,.2f}" for k, v in sorted(net.items())]
    print(f"{indent}{label}：{'，'.join(parts) if parts else '（无净分录）'}")
    return net


def assert_no_diff(lines, trade_id, indent="    "):
    """断言：这一轮对账里，本笔交易没有产生任何差异行 → 判为「平」。

    场景 6 要证明「冲正之后差异真的消失」，不能只靠读者从简报的「平 2 → 3」反推。
    这里直接检查日志：本 tradeId 一条差异行都没有，才算通过。
    """
    offending = [l for l in lines
                 if "[recon][P" in l and trade_id in l]
    if offending:
        print(f"{indent}断言失败：本笔仍被判为差异")
        for l in offending:
            print(f"{indent}  {l}")
        return False
    print(f"{indent}断言通过：本轮对账日志里没有 tradeId={trade_id} 的任何差异行 → 这笔已判为「平」")
    return True


# ---------------------------------------------------------------------------
# 场景
# ---------------------------------------------------------------------------

def scenario_baseline():
    print("=" * 78)
    print("场景 1 · baseline：正常转账，三本账应该全平")
    print("=" * 78)
    trade_id = settle()
    print(f"  发起转账：工行 A → 建行 B，¥{AMOUNT_CENTS / 100:,.2f}，tradeId={trade_id}")
    wait_booked(trade_id, CCB, 2)
    wait_booked(trade_id, ICBC, 2)

    print("\n  两边账本（对应文章第二节那张分录表）：")
    print_journal("工行", ICBC, trade_id)
    print_journal("建行", CCB, trade_id)

    offset = log_size()
    run_recon()
    time.sleep(1)
    print("\n  对账结果：")
    show_round(log_since(offset), trade_id)
    print("\n  → 预期：这笔不出现在任何差异里，简报显示「平」。")
    return trade_id


def scenario_missing():
    print("\n" + "=" * 78)
    print("场景 2 · missing：建行漏记 → 对账自动补记 → 闭环验收")
    print("=" * 78)
    trade_id = settle()
    print(f"  发起转账：tradeId={trade_id}")
    wait_booked(trade_id, CCB, 2)
    wait_booked(trade_id, ICBC, 2)

    print("\n  注入故障：建行入账通知丢失，清掉建行已记的分录")
    post(f"{CCB}/api/bank/fault/miss", {"tradeId": trade_id})
    print_journal("建行（注入后）", CCB, trade_id)

    offset = log_size()
    run_recon()
    time.sleep(1)
    print("\n  对账结果：")
    show_round(log_since(offset), trade_id)

    print("\n  补记后的建行账本（注意 entryId 的 COMP- 前缀 = 对账补记，不是原始入账）：")
    print_journal("建行", CCB, trade_id)
    print("\n  → 预期：判为 MISSING（漏记），自动补记两个科目，闭环验收通过。")
    print("     补记动作完全由结算记录决定（补哪侧、补多少都明确），所以能自动做。")
    return trade_id


def scenario_duplicate():
    print("\n" + "=" * 78)
    print("场景 3 · duplicate：建行重复入账 → 只标记，不自动冲正")
    print("=" * 78)
    trade_id = settle()
    print(f"  发起转账：tradeId={trade_id}")
    wait_booked(trade_id, CCB, 2)
    wait_booked(trade_id, ICBC, 2)

    print("\n  注入故障：绕过幂等，建行再记一组（模拟通知重复投递 + 消费端没做唯一性控制）")
    resp = post(f"{CCB}/api/bank/fault/duplicate", {
        "tradeId": trade_id, "fromBank": "ICBC", "toBank": "CCB",
        "amountCents": AMOUNT_CENTS, "settledAt": settled_at(trade_id),
    })
    print_journal("建行（注入后）", CCB, trade_id)

    offset = log_size()
    run_recon()
    time.sleep(1)
    print("\n  对账结果：")
    show_round(log_since(offset), trade_id)

    print("\n  → 预期：判为 DUPLICATE，只告警 + 给出人工裁决命令，不动账。")
    print("     tradeId 相同只能说明「多记了」，判断不出哪一组是正确的——")
    print("     自动冲正必须回答「冲哪一组」，而这个答案对账给不出来。")
    return trade_id, resp.get("entryIds", [])


def scenario_amount():
    print("\n" + "=" * 78)
    print("场景 4 · amount：工行金额记错 → 挂起等人工")
    print("=" * 78)
    trade_id = settle()
    print(f"  发起转账：tradeId={trade_id}，结算金额 ¥{AMOUNT_CENTS / 100:,.2f}")
    wait_booked(trade_id, CCB, 2)
    wait_booked(trade_id, ICBC, 2)

    wrong = AMOUNT_CENTS // 10   # 记成 1,000 元
    print(f"\n  注入故障：工行把这笔记成 ¥{wrong / 100:,.2f}（两条分录都用错误金额）")
    post(f"{ICBC}/api/bank/fault/wrong-amount", {
        "tradeId": trade_id, "fromBank": "ICBC", "toBank": "CCB",
        "amountCents": wrong, "settledAt": settled_at(trade_id),
    })
    entries = print_journal("工行（注入后）", ICBC, trade_id)

    # 关键观察：错误金额的两条分录自身仍然借贷平衡
    debit = sum(e["amountCents"] for e in entries if e["direction"] == "DEBIT")
    credit = sum(e["amountCents"] for e in entries if e["direction"] == "CREDIT")
    print(f"\n  工行本行试算平衡检查：借方合计 ¥{debit / 100:,.2f}，"
          f"贷方合计 ¥{credit / 100:,.2f} → {'平' if debit == credit else '不平'}")

    offset = log_size()
    run_recon()
    time.sleep(1)
    print("\n  对账结果：")
    show_round(log_since(offset), trade_id)

    print("\n  → 预期：判为 AMOUNT_MISMATCH，挂起等人工。")
    print("     注意上面那行：本行账是「平」的，试算平衡发现不了这个错。")
    print("     这正是文章第二节说的——金额同时记错，借贷合计仍然相等。")
    return trade_id


def scenario_pending():
    print("\n" + "=" * 78)
    print("场景 5 · pending：结算侧状态未定 → 两侧都判待查，不做任何修正")
    print("=" * 78)
    trade_id = settle()
    print(f"  发起转账：tradeId={trade_id}")
    wait_booked(trade_id, CCB, 2)
    wait_booked(trade_id, ICBC, 2)

    print("\n  注入故障：把结算记录改回 PENDING（结算中）")
    post(f"{SETTLEMENT}/api/settlement/fault/pending", {"tradeId": trade_id})
    rec = get(f"{SETTLEMENT}/api/settlement/{trade_id}")
    print(f"  结算记录现状：tradeId={rec['tradeId']}, status={rec['status']}")

    offset = log_size()
    run_recon()
    time.sleep(1)
    print("\n  对账结果：")
    show_round(log_since(offset), trade_id)

    print("\n  两家银行的分录都没被动过：")
    print_journal("工行", ICBC, trade_id)
    print_journal("建行", CCB, trade_id)
    print("\n  → 预期：两侧各报一条 NO_SETTLEMENT（待查），不补记、不冲正。")
    print("     权威源缺位时，宁可什么都不做也不猜。")
    return trade_id


def scenario_adjudicate():
    print("\n" + "=" * 78)
    print("场景 6 · adjudicate：人工裁决冲正多余那组 → 重新对账差异消失")
    print("=" * 78)
    trade_id, dup_ids = scenario_duplicate_quiet()
    if not dup_ids:
        print("  没拿到注入的重复分录 ID，跳过本场景")
        return None

    before = print_net("冲正前建行净分录（客户存款多出 ¥10,000，对不上结算）",
                       journal(CCB, trade_id), indent="    ")

    print("\n  人工看完账本，确认 DUP- 前缀那组是多余的，裁决冲正它：")
    print(f"    待冲正 entryIds = {dup_ids}")
    post(f"{CCB}/api/bank/fault/adjudicate-reverse",
         {"tradeId": trade_id, "entryIds": dup_ids})

    print("\n  冲正后的建行账本（原始分录一条没删，多了两条 REV- 反向分录）：")
    entries_after = print_journal("建行", CCB, trade_id)
    after = print_net("冲正后建行净分录（回到每个科目各 ¥10,000，与结算一致）",
                      entries_after, indent="    ")

    # 证据一：净分录口径的钱数对上了
    ok_net = all(after.get(k) == AMOUNT_CENTS for k in
                 ("customer_deposit", "pboc_settlement"))
    print(f"    净分录校验：{'通过' if ok_net else '不通过'}"
          f"（冲正前客户存款 ¥{before.get('customer_deposit', 0) / 100:,.2f}"
          f" → 冲正后 ¥{after.get('customer_deposit', 0) / 100:,.2f}）")

    offset = log_size()
    run_recon()
    time.sleep(1)
    print("\n  重新对账：")
    lines = log_since(offset)
    show_round(lines, trade_id)

    # 证据二：对账日志里这笔不再产生差异
    print()
    assert_no_diff(lines, trade_id)

    print("\n  → 结论：这笔重新判为「平」，不再出现在差异里。")
    print("     两条独立证据：①净分录的钱数回到与结算一致；②对账日志里这笔零差异。")
    print("     闭环验收按「净分录」重跑完整匹配，不是数分录条数——")
    print("     冲正是追加式的，条数从 4 条变 6 条，只有净分录能证明差异真的消失了。")
    return trade_id


def scenario_duplicate_quiet():
    """场景 6 的前半段：造一个重复入账，但不打印场景 3 那套解说"""
    trade_id = settle()
    print(f"  发起转账：tradeId={trade_id}")
    wait_booked(trade_id, CCB, 2)
    wait_booked(trade_id, ICBC, 2)

    resp = post(f"{CCB}/api/bank/fault/duplicate", {
        "tradeId": trade_id, "fromBank": "ICBC", "toBank": "CCB",
        "amountCents": AMOUNT_CENTS, "settledAt": settled_at(trade_id),
    })
    print("\n  注入重复入账后的建行账本：")
    print_journal("建行", CCB, trade_id)
    return trade_id, resp.get("entryIds", [])


SCENARIOS = {
    "baseline": scenario_baseline,
    "missing": scenario_missing,
    "duplicate": scenario_duplicate,
    "amount": scenario_amount,
    "pending": scenario_pending,
    "adjudicate": scenario_adjudicate,
}

DEFAULT_ORDER = ["baseline", "missing", "duplicate", "amount", "pending", "adjudicate"]


def preflight():
    """三个服务都在才算能跑；给出明确的启动指引，不要让读者对着 ConnectionRefused 猜"""
    targets = [("结算服务（含对账 Job）", SETTLEMENT),
               ("工行 bank-a", ICBC), ("建行 bank-b", CCB)]
    down = []
    for name, base in targets:
        try:
            urllib.request.urlopen(base + "/", timeout=3)
        except urllib.error.HTTPError:
            pass            # 有 HTTP 响应就说明服务起来了（404 也算）
        except Exception:
            down.append(f"{name} {base}")
    if down:
        print("以下服务没起来，先跑 ./start.sh：")
        for d in down:
            print(f"  - {d}")
        sys.exit(1)

    if not os.path.exists(SETTLEMENT_LOG):
        print(f"找不到结算服务日志 {SETTLEMENT_LOG}")
        print("日志由 start.sh 写入；如果你是手动 java -jar 起的，")
        print("场景仍能跑，但抓不到对账日志，请自行观察结算服务控制台。")


def main():
    parser = argparse.ArgumentParser(description="GYD C03 对账差异复现实验")
    parser.add_argument("--only", choices=list(SCENARIOS),
                        help="只跑指定场景")
    args = parser.parse_args()

    preflight()

    order = [args.only] if args.only else DEFAULT_ORDER
    print(__doc__.split("六个场景：")[0].strip())
    print(f"\n本次运行场景：{', '.join(order)}\n")

    # 跑全套时先复位，让简报从「共 1 笔」开始逐个场景递增，读者能对上每一步。
    # 单跑某个场景时不动数据，方便在自己的账本上复现某一类差异。
    if not args.only:
        reset_environment()

    for name in order:
        try:
            SCENARIOS[name]()
        except Exception as exc:
            print(f"\n场景 {name} 失败：{type(exc).__name__}: {exc}")
            raise
        print()

    print("=" * 78)
    print("全部场景跑完。四类差异的结局对照：")
    print("  MISSING           漏记      → 自动补记 + 闭环验收通过")
    print("  DUPLICATE         重复入账  → 挂起，等人工裁决冲哪一组")
    print("  AMOUNT_MISMATCH   金额不符  → 挂起，需回溯原始支付指令")
    print("  NO_SETTLEMENT     待查      → 不做任何修正，等下一批次")
    print("=" * 78)


if __name__ == "__main__":
    main()
