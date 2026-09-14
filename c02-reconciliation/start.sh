#!/bin/bash
# GYD C02 对账 Demo 一键启动
#
# 做四件事：
#   1. 启动基础设施（PostgreSQL + RabbitMQ）
#   2. 等待两者就绪
#   3. 构建两个可执行模块的 bootJar，后台启动三个进程：
#        settlement（结算 + 对账）→ :18080
#        bank 起两次（工行/建行）  → :18081 / :18082
#      日志写到 ./logs/，PID 写到 ./.pids/
#   4. 等待三个服务的健康检查通过
#
# 用法：
#   ./start.sh          构建 + 启动全部
#   ./start.sh --down   停掉三个服务与基础设施
#
# 为什么用 bootJar + java -jar，而不是 gradlew bootRun：
#   bootRun 的 Java 进程是 Gradle 守护进程 fork 出来的，脚本记下的 PID 是 Gradle
#   客户端的，kill 它并不会停掉真正在跑的服务（--down 会留僵尸）。自己 java -jar
#   才能拿到服务进程的真实 PID，起停都对得上。
#
# 注意：路径以「本脚本所在目录」为基准推导，从仓库根或 c02 目录调用都能正确定位 infra/。

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
INFRA="$REPO_ROOT/infra"
LOG_DIR="$SCRIPT_DIR/logs"
PID_DIR="$SCRIPT_DIR/.pids"
GRADLEW="$REPO_ROOT/gradlew"
MODULE="c02-reconciliation"

# Gradle 必须用 JDK 21（系统默认可能是更高版本，会抛 IllegalArgumentException）
export JAVA_HOME="${JAVA_HOME:-/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home}"
JAVA_BIN="$JAVA_HOME/bin/java"

# docker 可能不在当前 PATH（受限 shell / 定时任务里很常见），补上两个标准安装位
export PATH="$PATH:/usr/local/bin:/opt/homebrew/bin"

# 名称:端口:模块:profile（profile 为空表示不需要）
SERVICES=(
    "settlement:18080:settlement:"
    "bank-a:18081:bank:bank-a"
    "bank-b:18082:bank:bank-b"
)

stop_pid() {
    # $1 = 名称，$2 = pid。先温和 TERM，2 秒后还在就 KILL。
    local name="$1" pid="$2"
    [ -n "$pid" ] || return 0
    kill -0 "$pid" 2>/dev/null || return 0
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 10); do
        kill -0 "$pid" 2>/dev/null || { echo "  已停止 $name (pid=$pid)"; return 0; }
        sleep 0.2
    done
    kill -9 "$pid" 2>/dev/null || true
    echo "  已强制停止 $name (pid=$pid)"
}

down() {
    echo "=== 停止服务 ==="
    if [ -d "$PID_DIR" ]; then
        for f in "$PID_DIR"/*.pid; do
            [ -e "$f" ] || continue
            stop_pid "$(basename "$f" .pid)" "$(cat "$f")"
            rm -f "$f"
        done
    fi
    docker compose -f "$INFRA/postgres/docker-compose.yml" down
    docker compose -f "$INFRA/rabbitmq/docker-compose.yml" down
    echo "完成。数据库卷 gyd-postgres-data 保留；要彻底清空跑 docker volume rm gyd-postgres-data"
}

if [ "$1" = "--down" ]; then
    down
    exit 0
fi

mkdir -p "$LOG_DIR" "$PID_DIR"

container_running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

echo "=== 启动基础设施 ==="
# 已经在跑就别让 compose 去重建：compose 会比对镜像 tag，发现容器用的镜像和
# compose 里写的不一致就尝试重新拉取；离线/沙箱环境拉不到 docker.io 会直接失败，
# 但其实容器一直好好跑着。先探活，活着就跳过 compose up。
if container_running gyd-postgres && container_running gyd-rabbitmq; then
    echo "  gyd-postgres / gyd-rabbitmq 已在运行，跳过 compose up"
else
    docker compose -f "$INFRA/postgres/docker-compose.yml" up -d
    docker compose -f "$INFRA/rabbitmq/docker-compose.yml" up -d
fi

echo "=== 等待 PostgreSQL 就绪 ==="
until docker exec gyd-postgres pg_isready -U gyd > /dev/null 2>&1; do
    echo "  等待 postgres..."
    sleep 1
done
echo "  postgres OK"

echo "=== 等待 RabbitMQ 就绪 ==="
until docker exec gyd-rabbitmq rabbitmqctl status > /dev/null 2>&1; do
    echo "  等待 RabbitMQ..."
    sleep 1
done
echo "  RabbitMQ OK"

echo "=== 构建 bootJar ==="
# 先停掉上一轮的进程，否则 jar 被占用、且旧进程还在占端口
for f in "$PID_DIR"/*.pid; do
    [ -e "$f" ] || continue
    stop_pid "$(basename "$f" .pid)" "$(cat "$f")"
    rm -f "$f"
done
(cd "$REPO_ROOT" && "$GRADLEW" -q \
    "$MODULE:settlement:bootJar" "$MODULE:bank:bootJar")
echo "  构建完成"

jar_of() {
    # bootJar 产物名是 <模块名>-<version>.jar，用 glob 取，避免把 version 写死在脚本里
    ls "$SCRIPT_DIR/$1/build/libs/"*.jar 2>/dev/null | head -1
}

echo "=== 启动三个服务（日志在 $LOG_DIR） ==="
# 拆成子模块后，「哪个进程」由模块决定，不再靠 profile 切代码：
#   settlement 模块自己就是一个进程；bank 是同一份代码起两次，
#   profile 只选「这个实例是谁」（银行代码、端口、库名、队列名）。
for svc in "${SERVICES[@]}"; do
    IFS=':' read -r name port module profile <<< "$svc"
    log_file="$LOG_DIR/$name.log"
    jar="$(jar_of "$module")"
    if [ -z "$jar" ]; then
        echo "  找不到 $module 的 bootJar，构建可能失败了" >&2
        exit 1
    fi

    # 端口走命令行参数：沙箱注入的 SERVER__PORT 环境变量会通过 relaxed binding
    # 覆盖 application-{profile}.yml 里的 server.port，导致三个服务撞同一个端口
    args="--server.port=$port"
    [ -z "$profile" ] || args="--spring.profiles.active=$profile $args"

    # shellcheck disable=SC2086
    "$JAVA_BIN" -jar "$jar" $args > "$log_file" 2>&1 &
    echo $! > "$PID_DIR/$name.pid"
    echo "  $name (端口 $port, $module) → $log_file"
done

echo "=== 等待服务健康 ==="
# 判据是「端口能返回 HTTP 响应」，不挑路径：404 也说明 Spring Boot 已经起来了。
# 用 -w 取状态码而不是 -sf，否则 404 会被当成失败一直等下去。
for svc in "${SERVICES[@]}"; do
    IFS=':' read -r name port _ _ <<< "$svc"
    printf "  %-12s" "$name"
    for _ in $(seq 1 60); do
        pid="$(cat "$PID_DIR/$name.pid" 2>/dev/null)"
        if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
            echo "启动失败，进程已退出，看 $LOG_DIR/$name.log"
            exit 1
        fi
        code=$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' -m 2 "http://localhost:$port/" 2>/dev/null)
        code=${code:-000}
        if [ "$code" != "000" ]; then
            echo "OK (端口 $port, HTTP $code)"
            break
        fi
        sleep 1
        printf "."
    done
    echo ""
done

cat <<EOF

启动完成。接下来跑实验：
  python3 scripts/recon-experiment.py        # 依次造出四类差异并触发对账

或者手动走一遍：
  # 1) 发一笔正常转账（工行 → 建行，10,000 元）
  curl -X POST localhost:18080/api/settle -H 'Content-Type: application/json' \\
       -d '{"fromBank":"ICBC","toBank":"CCB","amountCents":1000000}'

  # 2) 看工行账本上记了什么（对应文章第二节那张分录表）
  curl -s 'localhost:18081/api/bank/journal/<tradeId>' | python3 -m json.tool

  # 3) 手动触发一次对账（不等定时任务）
  curl -X POST localhost:18080/api/recon/run

停止：
  ./start.sh --down
EOF
