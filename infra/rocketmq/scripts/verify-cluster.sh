#!/usr/bin/env bash
# 验证集群是否真的组起来了
#
#   ./scripts/verify-cluster.sh
#
# 只看正在运行的 broker（没启 broker-b 组时不会报错），逐个确认：
#   1. 注册到 NameServer 的 broker 列表
#   2. 每个 broker 实际生效的角色 / 刷盘 / 通告地址
#   3. 三个端口（listenPort、listenPort-2、listenPort+1）从宿主机是否可达
#   4. 从节点是否真的在从主节点同步
set -euo pipefail

cd "$(dirname "$0")/.."
DOCKER="${DOCKER_BIN:-docker}"

if [ ! -f .host-ip ]; then
  echo "缺少 .host-ip，先执行 ./scripts/render-conf.sh" >&2
  exit 1
fi
HOST_IP="$(cat .host-ip)"
NS="${HOST_IP}:9876"
IMG=apache/rocketmq:4.9.7

say() { printf '\n=== %s ===\n' "$1"; }

say "1. 注册到 NameServer 的 broker（${NS}）"
"$DOCKER" run --rm "$IMG" sh mqadmin clusterList -n "$NS" 2>/dev/null

say "2. 每个 broker 实际生效的配置"
for conf in conf/broker-*.conf; do
  name="$(basename "$conf" .conf)"          # 例如 broker-a-master
  cname="gyd-rmq-${name}"
  "$DOCKER" inspect "$cname" >/dev/null 2>&1 || continue   # 没运行就跳过
  port="$(sed -n 's/^listenPort *= *//p' "$conf" | tr -d ' ')"
  printf -- '--- %s  (%s:%s)\n' "$name" "$HOST_IP" "$port"
  "$DOCKER" run --rm "$IMG" sh mqadmin getBrokerConfig -b "${HOST_IP}:${port}" 2>/dev/null \
    | grep -E '^[[:space:]]*(brokerName|brokerId|brokerRole|flushDiskType|brokerIP1|listenPort|haListenPort)[[:space:]]*=' \
    | sed 's/^/    /' || echo "    (取不到配置)"
done

say "3. 宿主机到各端口的连通性"
probe() {  # $1=端口 $2=说明
  if nc -z -G 2 "$HOST_IP" "$1" 2>/dev/null; then
    echo "  OK   $2 ($HOST_IP:$1)"
  else
    echo "  FAIL $2 ($HOST_IP:$1)"
  fi
}
for conf in conf/broker-*.conf; do
  name="$(basename "$conf" .conf)"
  "$DOCKER" inspect "gyd-rmq-${name}" >/dev/null 2>&1 || continue
  port="$(sed -n 's/^listenPort *= *//p' "$conf" | tr -d ' ')"
  probe "$port"                "${name} listenPort"
  probe "$((port - 2))"        "${name} 热路径(listenPort-2)"
  probe "$((port + 1))"        "${name} HA(listenPort+1)"
done
probe 9876 "namesrv"

say "4. 从节点是否在同步主节点数据"
found=0
for conf in conf/broker-*-slave.conf; do
  [ -e "$conf" ] || continue
  name="$(basename "$conf" .conf)"
  cname="gyd-rmq-${name}"
  "$DOCKER" inspect "$cname" >/dev/null 2>&1 || continue
  found=1
  printf -- '--- %s\n' "$name"
  "$DOCKER" exec "$cname" sh -c \
    "grep 'Update slave .* from master' /home/rocketmq/logs/rocketmqlogs/broker.log 2>/dev/null | tail -2 | sed 's/^/    /'" \
    || echo "    (日志里还没有同步记录，稍等几秒再看)"
done
[ "$found" = 1 ] || echo "  (没在跑从节点)"
