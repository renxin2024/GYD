package cn.renxinblog.gyd.c04;

import org.apache.rocketmq.client.producer.DefaultMQProducer;

import java.nio.charset.StandardCharsets;

/**
 * demo 的统一入口与共享配置。
 *
 * <p>用法：{@code ./gradlew :c04-rocketmq-architecture:run --args="send"}
 *
 * <p>NameServer 地址优先取环境变量 {@code RMQ_NAMESRV}，否则用本机练习集群的地址。
 * 之所以写成"可覆盖 + 默认值"，是因为宿主机访问 broker 必须走宿主的 LAN IP
 * （宿主机解析不了容器名，也连不通容器 IP），换网络后这个地址会变。
 */
public final class DemoRunner {

    public static final String DEFAULT_NAMESRV = "192.168.31.132:9876";

    // 主题名集中在这里：跑 demo 前需要先用 mqadmin updateTopic 建好（-r 4 -w 4），
    // 这样队列数是确定的，观察落点分布才有可比性。
    public static final String TOPIC_SEND = "gyd-c03-send";
    public static final String TOPIC_ORDER = "gyd-c03-order";
    public static final String TOPIC_FILTER = "gyd-c03-filter";
    public static final String TOPIC_DELAY = "gyd-c03-delay";
    public static final String TOPIC_TX = "gyd-c03-tx";

    private DemoRunner() {
    }

    public static String namesrvAddr() {
        String fromEnv = System.getenv("RMQ_NAMESRV");
        return (fromEnv == null || fromEnv.isBlank()) ? DEFAULT_NAMESRV : fromEnv;
    }

    /** 每个 demo 一个 banner，方便从长日志里切段引用。 */
    public static void head(String demo) {
        System.out.println();
        System.out.println("===== demo: " + demo + " =====");
    }

    /**
     * 统一输出前缀，便于 grep 出可引用的证据行。
     *
     * <p>注意这里必须把前缀拼进 format 再传 args：如果写成
     * {@code printf("[%s] " + format, prefix, args)}，Java 会把 prefix 和整个 args 数组
     * 当成两个实参塞进 varargs，格式化时第一个 %d 拿到的就是 Object[]。
     */
    public static void say(String prefix, String format, Object... args) {
        System.out.printf("[" + prefix + "] " + format + "%n", args);
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** 起一个 producer 并把本篇要讲的三个客户端默认值打出来，省得每个 demo 各打一遍。 */
    static DefaultMQProducer producer(String group) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setNamesrvAddr(namesrvAddr());
        producer.start();
        return producer;
    }

    public static void main(String[] args) throws Exception {
        String demo = args.length > 0 ? args[0] : "help";
        try {
            switch (demo) {
                case "route" -> AddressingDemos.run();
                case "autocreate" -> AddressingDemos.autoCreateProbe();
                case "send" -> ProducerDemos.syncAsyncOnewayBatch();
                case "ordered" -> ProducerDemos.ordered();
                case "failmode" -> ProducerDemos.sendStatusProbe();
                case "consume" -> ConsumerDemos.concurrent();
                case "orderly" -> ConsumerDemos.orderly();
                case "filter-tag" -> FeatureDemos.filterByTag();
                case "filter-sql" -> FeatureDemos.filterBySql();
                case "delay" -> FeatureDemos.delay();
                case "tx" -> FeatureDemos.transaction();
                case "vip" -> VipChannelProbe.run(args);
                case "offsetprobe" -> OffsetWindowProbe.run(args);
                default -> System.out.println("""
                        用法: run --args="<demo>"

                          route      寻址：拉路由、客户端默认间隔、broker 地址表
                          autocreate 自动建 Topic 的队列数（对照显式建 Topic）
                          send       同步 / 异步 / 单向 / 批量，并打印队列落点分布
                          ordered    顺序发送（MessageQueueSelector 固定队列）
                          failmode   单次发送的 SendStatus（配合停/起从节点观察）
                          consume    并发消费：线程数、队列数、流控默认值
                          orderly    顺序消费：同一队列是否只有一个线程
                          filter-tag tag 过滤
                          filter-sql SQL92 过滤（需要 broker 打开 enablePropertyFilter）
                          delay      固定级延迟：标称延迟 vs 实测延迟
                          tx         事务消息：半消息、本地事务、回查
                          vip        VIP 通道对照：默认连哪个端口 + 端口不可达时的异常
                                     （vip [on|off] [发送前持有秒数] [发送后持有秒数]）
                          offsetprobe 位点窗口探针：kill -9 之后哪些消息会重复消费
                                     （offsetprobe <produce|consume> <条数>）
                        """);
            }
        } finally {
            System.out.flush();
        }
        // RocketMQ 客户端留下非守护线程，不显式退出会挂住 gradle。
        System.exit(0);
    }
}
