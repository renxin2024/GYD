#!/usr/bin/env bash
#
# 让 rabbitmq2 / rabbitmq3 加入 rabbitmq1，组成 3 节点集群。
#
# 前提：docker compose up -d 已把三个节点启动起来。
# 说明：join 会先 reset 目标节点（清掉它本地已有数据），再从集群同步，
#       所以只需在首次搭建或 `down -v` 之后执行；正常重启会自动保持集群成员关系。
#
set -euo pipefail

DOCKER="${DOCKER:-docker}"
LEADER_NODE="rabbit@rabbitmq1"

wait_ready() {
  local container="$1"
  echo ">>> 等待 ${container} 就绪 ..."
  for _ in $(seq 1 60); do
    if $DOCKER exec "$container" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
      echo "    ${container} 已就绪"
      return 0
    fi
    sleep 2
  done
  echo "    ${container} 未在预期时间内就绪" >&2
  return 1
}

join_node() {
  local container="$1"
  wait_ready "$container"
  echo ">>> ${container} 加入集群 ${LEADER_NODE}"
  $DOCKER exec "$container" rabbitmqctl stop_app
  $DOCKER exec "$container" rabbitmqctl reset
  $DOCKER exec "$container" rabbitmqctl join_cluster "$LEADER_NODE"
  $DOCKER exec "$container" rabbitmqctl start_app
}

join_node gyd-rabbitmq2
join_node gyd-rabbitmq3

echo ">>> 集群状态"
$DOCKER exec gyd-rabbitmq1 rabbitmqctl cluster_status
