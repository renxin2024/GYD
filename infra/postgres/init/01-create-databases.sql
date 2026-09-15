-- PostgreSQL 初始化：为 GYD C03 对账 demo 创建三个独立数据库
--
-- 为什么是三个独立库、而不是一个库分三张表：
-- 「三方各自持有账本、互相看不到对方的数据」是对账能成立的前提。demo 里用独立数据库
-- 把这个边界做实——结算服务连 gyd_c03_settlement，工行连 gyd_c03_bank_a，
-- 建行连 gyd_c03_bank_b，谁都查不到别人库里的分录，只能像生产环境那样走 REST 接口拉。
--
-- 注意：CREATE DATABASE 不能在事务块中执行。Postgres 官方镜像对
-- docker-entrypoint-initdb.d 下的 .sql 是逐条执行、不包在一个事务里的，
-- 所以下面三条 CREATE DATABASE 可以正常建库。

CREATE DATABASE gyd_c03_settlement;
CREATE DATABASE gyd_c03_bank_a;
CREATE DATABASE gyd_c03_bank_b;
