#!/bin/bash
# GYD C02 对账 Demo 一键启动
# 1. 启动基础设施（PostgreSQL + RabbitMQ）
# 2. 等待就绪
# 3. 以不同 profile 启动三个 Spring Boot 服务

set -e

echo "=== 启动基础设施 ==="
docker compose -f ../../infra/postgres/docker-compose.yml up -d
docker compose -f ../../infra/rabbitmq/docker-compose.yml up -d

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

echo "=== 启动服务 ==="
echo "启动方式（三个独立终端各开一个）："
echo "  ./gradlew c02-reconciliation:bootRun --args='--spring.profiles.active=settlement'"
echo "  ./gradlew c02-reconciliation:bootRun --args='--spring.profiles.active=bank-a'"
echo "  ./gradlew c02-reconciliation:bootRun --args='--spring.profiles.active=bank-b'"
echo ""
echo "或使用 Gradle parallel（在同一台机器上）："
echo "  ./gradlew c02-reconciliation:bootRun -Dspring.profiles.active=settlement &"
echo "  ./gradlew c02-reconciliation:bootRun -Dspring.profiles.active=bank-a &"
echo "  ./gradlew c02-reconciliation:bootRun -Dspring.profiles.active=bank-b &"