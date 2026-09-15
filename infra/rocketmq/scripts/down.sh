#!/usr/bin/env bash
# 停止集群
#
#   ./scripts/down.sh        # 停止，数据保留在命名卷
#   ./scripts/down.sh -v     # 停止并清空数据卷
#
# 固定带上 --profile b：docker compose down 只处理「当前文件里启用的服务」，
# 不带 profile 的话用 ./scripts/up.sh b 起来过的 broker-b 容器会残留，
# 还会占着网络删不掉。
set -euo pipefail

cd "$(dirname "$0")/.."
DOCKER="${DOCKER_BIN:-docker}"

"$DOCKER" compose --profile b down "$@"
