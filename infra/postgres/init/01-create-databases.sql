-- PostgreSQL 初始化：为 GYD C02 对账 demo 创建三个独立数据库
-- 注意：CREATE DATABASE 不能在事务块中执行（docker-entrypoint-initdb.d 默认在事务中运行）
-- 所以这里需要每个 CREATE DATABASE 单独提出来。但 Postgres Alpine + init 脚本的多语句逻辑
-- 我们简化：创建一个 gyd_c02 数据库，三个表分在不同的 schema 里。
-- （demo 阶段不追求完全隔离的多数据库，schema 级别隔离足够演示对账）

CREATE DATABASE gyd_c02_settlement;
CREATE DATABASE gyd_c02_bank_a;
CREATE DATABASE gyd_c02_bank_b;