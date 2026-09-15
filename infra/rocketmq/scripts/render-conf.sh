#!/usr/bin/env bash
# 用宿主机局域网 IP 渲染 conf/*.conf.tmpl → conf/*.conf，并记下这个 IP
#
# 为什么要这一步：broker 的对外通告地址（brokerIP1）必须是宿主机也能到达的地址。
# 容器名只在容器网络内可解析，宿主客户端拿着 broker-a-master:10911 是连不上的，
# 所以 conf 里得写宿主机 IP —— 而 IP 会随网络环境变，不能硬编码进仓库。
#
# 用法：
#   ./scripts/render-conf.sh                                  # 自动探测默认路由网卡的 IP
#   ROCKETMQ_HOST_IP=192.168.1.20 ./scripts/render-conf.sh    # 手动指定
set -euo pipefail

cd "$(dirname "$0")/.."

detect_host_ip() {
  local iface
  iface=$(route -n get default 2>/dev/null | awk '/interface:/{print $2}' | head -1)
  [ -n "${iface:-}" ] && ipconfig getifaddr "$iface" 2>/dev/null
}

HOST_IP="${ROCKETMQ_HOST_IP:-$(detect_host_ip || true)}"

if [ -z "${HOST_IP:-}" ]; then
  echo "无法自动探测宿主机 IP。请显式指定，例如：" >&2
  echo "  ROCKETMQ_HOST_IP=192.168.1.20 $0" >&2
  exit 1
fi

mkdir -p conf
for tmpl in conf/*.conf.tmpl; do
  sed "s/__HOST_IP__/${HOST_IP}/g" "$tmpl" > "${tmpl%.tmpl}"
done

# verify-cluster.sh 复用这个值，避免两处各探测一次
echo "$HOST_IP" > .host-ip

echo "已按宿主机 IP ${HOST_IP} 渲染配置："
ls -1 conf/*.conf | sed 's/^/  /'
