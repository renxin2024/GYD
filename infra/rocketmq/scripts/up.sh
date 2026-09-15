#!/usr/bin/env bash
# 启动 RocketMQ 集群
#
#   ./scripts/up.sh          # 默认 3 个容器：namesrv + broker-a 主/从
#   ./scripts/up.sh b        # 追加 broker-b 组，共 5 个容器
#   DOCKER_BIN=/usr/local/bin/docker ./scripts/up.sh   # docker 不在 PATH 时
#
# 分三步走，两个坑都在这里填掉：
#   1. compose create  —— 先把容器和数据卷建出来，先不启动
#   2. 改卷属主        —— Docker 新建命名卷属主是 root，而镜像以 uid 3000 运行；
#                        不改的话 broker 写不了 store（存储初始化失败，进程以 -3
#                        退出）和 logs（日志全丢）
#   3. compose up -d   —— 启动，并按 depends_on 的健康检查顺序拉起
set -euo pipefail

cd "$(dirname "$0")/.."
DOCKER="${DOCKER_BIN:-docker}"

# macOS 自带 bash 3.2 在 set -u 下展开空数组会报 unbound variable，
# 所以把 docker compose 本身也塞进数组，保证它永远非空。
COMPOSE=("$DOCKER" compose)
case "${1:-}" in
  b|full) COMPOSE+=(--profile b) ;;
  "") ;;
  *) echo "用法: $0 [b]" >&2; exit 2 ;;
esac

./scripts/render-conf.sh

echo
echo "创建容器与数据卷……"
"${COMPOSE[@]}" create

echo
echo "修正数据卷属主（root → 3000:3000）……"
for vol in $("$DOCKER" volume ls -q --filter name=gyd-rmq-); do
  "$DOCKER" run --rm -v "${vol}":/v alpine:latest chown -R 3000:3000 /v >/dev/null
  echo "  $vol"
done

echo
echo "启动……"
"${COMPOSE[@]}" up -d

echo
echo "等待节点就绪（broker 注册需要几秒）……"
sleep 8
"${COMPOSE[@]}" ps

echo
./scripts/verify-cluster.sh || true
