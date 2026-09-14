package cn.renxinblog.gyd.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 本篇（GYD 前置篇：RabbitMQ 的架构与特性）所有 demo 的统一入口与共享工具。
 *
 * <p>用法：{@code ./gradlew :rabbitmq-architecture:run --args="<demo>"}；
 * 末尾追加 {@code keep} 可在跑完后保留队列与交换机，方便去管理台核对。
 *
 * <p>连接参数可用环境变量覆盖：{@code RABBITMQ_HOST / PORT / MGMT_PORT / USER / PASS / VHOST}，
 * 默认对应 {@code infra/rabbitmq/docker-compose.yml} 里的本机容器。
 *
 * <p>本篇所有 AMQP 资源统一用 {@code gyd.ra.} 前缀，与 c01 的 {@code gyd.c01.*}、
 * c02 的 {@code gyd.c02.*} 隔离，互不干扰。
 *
 * <h2>一条贯穿全部 demo 的声明约定</h2>
 * 队列一律声明为 {@code durable = true, exclusive = false, autoDelete = false}。
 * 原因是本部署（4.3.5）把「既非持久化、又非排他」的队列列为<b>默认拒绝</b>的弃用特性
 * （{@code rabbitmq-diagnostics list_deprecated_features} 里
 * {@code transient_nonexcl_queues = denied_by_default}），沿用老教程里的
 * {@code durable=false} 会直接撞上这个报错。这也正是第七节要讲的那条现象的一个实例。
 */
public final class DemoRunner {

    public static final String HOST = env("RABBITMQ_HOST", "127.0.0.1");
    public static final int AMQP_PORT = Integer.parseInt(env("RABBITMQ_PORT", "5672"));
    public static final int MGMT_PORT = Integer.parseInt(env("RABBITMQ_MGMT_PORT", "15672"));
    public static final String USER = env("RABBITMQ_USER", "admin");
    public static final String PASS = env("RABBITMQ_PASS", "admin123");
    public static final String VHOST = env("RABBITMQ_VHOST", "/");

    /** 本篇所有队列/交换机的前缀，和 c01 的 gyd.c01.*、c02 的 gyd.c02.* 互不干扰。 */
    public static final String P = "gyd.ra.";

    /** 由命令行参数 {@code keep} 决定：跑完是否保留资源。 */
    public static boolean KEEP = false;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private static final Pattern JSON_NAME = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]*)\"");

    private DemoRunner() {
    }

    // ------------------------------------------------------------------ 入口

    public static void main(String[] args) throws Exception {
        String demo = args.length > 0 ? args[0] : "help";
        KEEP = Arrays.asList(args).contains("keep") || Arrays.asList(args).contains("--keep");
        try {
            switch (demo) {
                case "topology" -> TopologyDemos.run();
                case "route" -> RoutingDemos.route();
                case "default-exchange" -> RoutingDemos.defaultExchange();
                case "queue-types" -> QueueTypeDemos.run();
                case "consume-ack" -> ConsumeAckDemos.run();
                case "ttl" -> TtlDemos.run();
                case "dlx" -> DlxDemos.run();
                case "max-length" -> MaxLengthDemos.run();
                case "precondition" -> PreconditionDemos.run();
                case "persistence-write" -> PersistenceDemos.write();
                case "persistence-read" -> PersistenceDemos.read();
                default -> System.out.println(HELP);
            }
        } finally {
            System.out.flush();
        }
        // 客户端会留下非守护线程（连接心跳、消费者分发线程池），不显式退出会挂住 gradle。
        System.exit(0);
    }

    private static final String HELP = """
            用法: run --args="<demo> [keep]"

              topology         三层作用域（vhost/connection/channel）+ 信道级错误隔离
                               + 连接级错误 + 并发共享一条 channel 的对照
              route            同一路由键在 direct/fanout/topic 下的落点对比（含落 0 个队列）
              default-exchange 空串交换机名发布的落点，以及对它执行 bind 的结果
              queue-types      classic/quorum 的声明、改 x-queue-type 的结果、stream 的两次读
              consume-ack      未 ack 的消息在服务端的状态；prefetch 与未确认消息上限
              ttl              队列级 TTL、per-message TTL、两者取较小值、队头规则、requeue 后的到期时间
              dlx              四种死信触发；含「队列级 TTL 到期是否进死信」与「整队过期不死信」
              max-length       drop-head / reject-publish / reject-publish-dlx 三档溢出
              precondition     声明被拒的三种情形与它们关掉的是 channel 还是 connection

              下面两条要配对跑，中间由人重启 broker——因此不属于上面那套「一条命令一个段」的批量：
              persistence-write 往同一个 durable 队列里放 2 条 deliveryMode=2 + 1 条 deliveryMode=1
              persistence-read  重启之后重声明并抽干队列，看少的是哪一条

              末尾加 keep 保留本轮资源（不去删队列与交换机），便于去管理台核对。
            """;

    // ------------------------------------------------------------------ 基础工具

    public static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    public static Connection newConnection() throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(HOST);
        f.setPort(AMQP_PORT);
        f.setUsername(USER);
        f.setPassword(PASS);
        f.setVirtualHost(VHOST);
        f.setConnectionTimeout(10_000);
        return f.newConnection("gyd-rabbitmq-architecture");
    }

    /** 每个 demo 一个 banner，方便从长日志里切段引用。 */
    public static void head(String demo) {
        System.out.println();
        System.out.println("===== demo: " + demo + " =====");
    }

    /** 统一输出前缀，便于 grep 出可引用的证据行。 */
    public static void say(String tag, String format, Object... args) {
        System.out.printf("[" + tag + "] " + format + "%n", args);
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 变长参数版 Map 构造，省掉 Map.of 的 10 对上限与「不许 null 值」两条限制。 */
    public static Map<String, Object> args(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }

    public static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** 上一轮清理或一次意外之后，拿一条可用信道回来（不用关心是哪条）。 */
    public static Channel reopen(Connection conn, Channel ch) {
        if (ch != null && ch.isOpen()) {
            return ch;
        }
        try {
            return conn.createChannel();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void closeQuietly(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception ignored) {
            // 收尾时的异常不该盖住正文结论
        }
    }

    // ------------------------------------------------------------------ 消息收发

    public static void publish(Channel ch, String queue, String body) throws IOException {
        ch.basicPublish("", queue, null, utf8(body));
    }

    public static void publish(Channel ch, String exchange, String routingKey, String body) throws IOException {
        ch.basicPublish(exchange, routingKey, null, utf8(body));
    }

    /** 带 per-message TTL 的发布：{@code expiration} 是毫秒数的字符串形式。 */
    public static void publishWithExpiration(Channel ch, String queue, String body, String expirationMs)
            throws IOException {
        ch.basicPublish("", queue, new AMQP.BasicProperties.Builder().expiration(expirationMs).build(), utf8(body));
    }

    /** 取一条（pull 模型）；队列空返回 null。 */
    public static String getOnce(Channel ch, String queue, boolean autoAck) throws IOException {
        var resp = ch.basicGet(queue, autoAck);
        return resp == null ? null : new String(resp.getBody(), StandardCharsets.UTF_8);
    }

    /**
     * 用 basicConsume（push 模型）收最多 {@code expect} 条，最多等 {@code timeoutMs}。
     * 返回实际收到的消息体（可能少于 expect——这正是「没有消息」与「消息还没来」的区别所在）。
     */
    public static List<String> drain(Channel ch, String queue, boolean autoAck,
                                     Map<String, Object> consumeArgs, int expect, long timeoutMs) throws Exception {
        List<String> got = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(expect);
        DeliverCallback onDelivery = (consumerTag, delivery) -> {
            got.add(new String(delivery.getBody(), StandardCharsets.UTF_8));
            if (!autoAck) {
                try {
                    ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (IOException ignored) {
                    // 信道在收尾阶段被关是正常的
                }
            }
            latch.countDown();
        };
        String tag = (consumeArgs == null)
                ? ch.basicConsume(queue, autoAck, onDelivery, consumerTag -> { })
                : ch.basicConsume(queue, autoAck, consumeArgs, onDelivery, consumerTag -> { });
        latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        try {
            ch.basicCancel(tag);
        } catch (Exception ignored) {
            // 同上
        }
        return new ArrayList<>(got);
    }

    // ------------------------------------------------------------------ 资源声明与清理

    /**
     * 本篇统一的队列声明姿势：durable=true / exclusive=false / autoDelete=false。
     * 见类注释里的「一条贯穿全部 demo 的声明约定」。
     */
    public static void declareQueue(Channel ch, String queue, Map<String, Object> extra) throws IOException {
        ch.queueDeclare(queue, true, false, false, extra);
    }

    /** 队列当前深度（服务端 ready 消息数）。用被动声明取，不产生副作用。 */
    public static int depth(Channel ch, String queue) throws IOException {
        return ch.queueDeclarePassive(queue).getMessageCount();
    }

    /**
     * 删除本轮自己建出来的资源。
     *
     * <p>优先走管理 API：AMQP 删一个不存在的资源会返回 404，而 404 是<b>信道级</b>错误，
     * 会把信道关掉，后面几个删除就全部失败了。管理 API 没删成功时才回退到 AMQP（用传进来的
     * {@code ch}），这时才可能出现上面那个连锁。
     */
    public static void cleanup(Channel ch, String[] queues, String[] exchanges) {
        if (KEEP) {
            say("keep", "按参数保留资源：队列 %s；交换机 %s",
                    Arrays.toString(queues), Arrays.toString(exchanges));
            return;
        }
        for (String q : queues) {
            if (mgmtDelete("/queues/%2F/" + enc(q))) {
                continue;
            }
            try {
                ch.queueDelete(q);
            } catch (Exception e) {
                say("clean", "删除队列 %s 失败：%s", q, e.getMessage());
            }
        }
        for (String x : exchanges) {
            if (mgmtDelete("/exchanges/%2F/" + enc(x))) {
                continue;
            }
            try {
                ch.exchangeDelete(x);
            } catch (Exception e) {
                say("clean", "删除交换机 %s 失败：%s", x, e.getMessage());
            }
        }
        say("clean", "已删除本轮资源：%d 个队列 / %d 个交换机", queues.length, exchanges.length);
    }

    /**
     * 清掉上一轮遗留的本篇资源。
     *
     * <p>走管理 HTTP API 而不是 AMQP 的 {@code queueDelete}：AMQP 删一个不存在的队列会返回
     * 404，而 404 是<b>信道级</b>错误——信道会被 broker 关掉，后续操作全部失败。用管理 API
     * 就绕开了「删不存在的资源会把信道弄死」这件事。
     */
    public static int mgmtPurgePrefix() {
        int n = 0;
        for (String name : jsonNames(mgmt("/queues/%2F"))) {
            if (name.startsWith(P) && mgmtDelete("/queues/%2F/" + enc(name))) {
                n++;
            }
        }
        for (String name : jsonNames(mgmt("/exchanges/%2F"))) {
            if (name.startsWith(P) && mgmtDelete("/exchanges/%2F/" + enc(name))) {
                n++;
            }
        }
        return n;
    }

    // ------------------------------------------------------------------ 错误呈现

    /**
     * 把一个协议错误归类成「信道级 / 连接级」，并回读 connection 与 channel 的存活状态。
     *
     * <p>归类依据不是错误文本，而是 broker 关掉的是哪一个实体：{@code AMQP.Channel.Close}
     * 表示关的是信道（soft），{@code AMQP.Connection.Close} 表示关的是连接（hard）。
     */
    public static void reportError(String tag, Throwable t, Connection conn, Channel ch) {
        say(tag, "%s ⇒ %s", t.getClass().getSimpleName(), classify(t));
        say(tag, "   之后: connection.isOpen=%s, channel.isOpen=%s",
                conn == null ? "?" : conn.isOpen(), ch == null ? "?" : ch.isOpen());
    }

    /**
     * 把协议错误归类成一行「信道级 / 连接级 + 错误码 + 错误文本」。
     *
     * <p>归类依据不是错误文本，而是 broker 关掉的是哪一个实体：{@code AMQP.Channel.Close}
     * 表示关的是信道（soft），{@code AMQP.Connection.Close} 表示关的是连接（hard）。
     */
    public static String classify(Throwable t) {
        Throwable c = t;
        while (c != null && !(c instanceof ShutdownSignalException)) {
            c = c.getCause();
        }
        if (c instanceof ShutdownSignalException s) {
            Object reason = s.getReason();
            if (reason instanceof AMQP.Channel.Close cc) {
                return "信道级（soft）｜" + cc.getReplyCode() + " " + cc.getReplyText();
            }
            if (reason instanceof AMQP.Connection.Close cc) {
                return "连接级（hard）｜" + cc.getReplyCode() + " " + cc.getReplyText();
            }
            return "未归类｜" + reason;
        }
        return "未捕获到 ShutdownSignalException｜" + t.getClass().getSimpleName() + ": " + t.getMessage();
    }

    // ------------------------------------------------------------------ 管理 API

    /**
     * URL 编码一个资源名，供管理 API 的<b>路径段</b>使用。
     *
     * <p>{@link URLEncoder} 按表单规则把空格编成 {@code +}，但这里进的是路径段而不是查询串，
     * {@code +} 会被当作字面量加号，删不掉带空格的队列名。所以统一改写成 {@code %20}。
     */
    public static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * 明确指定要哪几列，避免依赖「默认返回哪些字段」这个会随版本变的约定。
     *
     * <p>踩过的坑：不带 {@code columns} 时同一接口的返回体形态并不稳定——有时是包含
     * {@code messages} 的完整对象，有时是只含 {@code arguments / auto_delete / consumer_details /
     * garbage_collection} 的短对象，后者取不到 {@code messages}。加上这一串白名单之后，
     * 返回体只剩这 6 个键，读数不再取决于服务端心情。
     */
    private static final String COLUMNS_QUEUE =
            "?columns=name,type,messages,messages_ready,messages_unacknowledged,consumers";

    /** GET 管理 API；非 200 一律返回空串，调用方据此判断「没取到」。 */
    public static String mgmt(String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create("http://" + HOST + ":" + MGMT_PORT + "/api" + path))
                    .header("Authorization", basicAuth())
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200 ? resp.body() : "";
        } catch (Exception e) {
            return "";
        }
    }

    /** DELETE 管理 API；200 与 204 都算成功。 */
    public static boolean mgmtDelete(String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create("http://" + HOST + ":" + MGMT_PORT + "/api" + path))
                    .header("Authorization", basicAuth())
                    .timeout(Duration.ofSeconds(10))
                    .DELETE()
                    .build();
            int code = HTTP.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
            return code == 200 || code == 204;
        } catch (Exception e) {
            return false;
        }
    }

    private static String basicAuth() {
        return "Basic " + Base64.getEncoder()
                .encodeToString((USER + ":" + PASS).getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> jsonNames(String json) {
        List<String> out = new ArrayList<>();
        Matcher m = JSON_NAME.matcher(json);
        while (m.find()) {
            out.add(m.group(1));
        }
        return out;
    }

    /**
     * 从管理 API 的 JSON 里取一个标量字段；取不到返回 "?"。
     *
     * <p>刻意不用正则：这份 JSON 里既有 {@code "messages":0} 这样的数字、也有
     * {@code "type":"classic"} 这样的字符串，还有 {@code "messages_details":{...}} 这种
     * 同前缀的兄弟键。按「键名 + 冒号」定位更直观，也免得把兄弟键误当成目标键。
     */
    public static String field(String json, String name) {
        String key = "\"" + name + "\":";
        int i = json.indexOf(key);
        if (i < 0) {
            return "?";
        }
        int v = i + key.length();
        if (v >= json.length()) {
            return "?";
        }
        if (json.charAt(v) == '"') {
            int end = json.indexOf('"', v + 1);
            return end < 0 ? "?" : json.substring(v + 1, end);
        }
        int end = v;
        while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') {
            end++;
        }
        return json.substring(v, end);
    }

    /**
     * 打印一个队列在管理端看到的状态。正文里「未 ack 的消息仍在服务端」这类结论都以此为准：
     * {@code messages} = {@code ready} + {@code unacked}。
     */
    /**
     * 打印一个队列的状态，<b>两个来源刻意分列</b>，因为它们的实时性不同：
     *
     * <ul>
     *   <li><b>AMQP 实时读</b>：{@code queue.declare-ok} 直接取队列进程里的 ready 计数与消费者数，
     *       是当前值。本 demo 里两次并发发布之后 300ms 内就收敛到 1600，靠的就是这一列。</li>
     *   <li><b>管理端快照</b>：字段来自统计采集（默认 5 秒一轮），<b>是快照不是实时值</b>。
     *       实测教训：队列级 TTL 那条用例里，管理端在 t=4.1s 仍报 {@code messages=3}，
     *       而紧接着起的消费者一条都读不到——快照落后于真实状态。</li>
     * </ul>
     *
     * <p>另外，刚声明、尚无活动的队列可能根本没有统计行，这时就算指定了 {@code columns}
     * 也只回 {@code {"name":...,"type":...}} 两个键（面板上一片空白也是这个原因）。
     *
     * <p>所以：<b>凡是有时间要求的断言一律引用 AMQP 那一列</b>，管理端那一列只作旁证；
     * 需要 {@code unacked} 这类 AMQP 拿不到的字段时才依赖管理端，并留出等待统计的余量。
     *
     * <p>这里<b>刻意不重试等待统计行</b>：一重试就把时间轴撑长，日志里原来的「t=1.9s」会变成
     * 「t=3.1s」，读数和它自称的时刻就对不上了。取不到就如实写「取不到」。
     */
    public static void state(String tag, Channel ch, String queue) {
        String live;
        String liveConsumers;
        try {
            AMQP.Queue.DeclareOk ok = ch.queueDeclarePassive(queue);
            live = String.valueOf(ok.getMessageCount());
            liveConsumers = String.valueOf(ok.getConsumerCount());
        } catch (Exception e) {
            live = "读不到(" + e.getClass().getSimpleName() + ")";
            liveConsumers = "?";
        }

        String json = mgmt("/queues/%2F/" + enc(queue) + COLUMNS_QUEUE);
        String msgs = json.isEmpty() ? "?" : field(json, "messages");

        if (json.isEmpty()) {
            say(tag, "队列 %s ｜ AMQP 实时 ready=%s consumers=%s ｜ 管理端无该队列（已删？）",
                    queue, live, liveConsumers);
            return;
        }
        say(tag, "队列 %s ｜ type=%s ｜ AMQP 实时 ready=%s consumers=%s ｜ 管理端快照 messages=%s (ready=%s, unacked=%s)",
                queue, field(json, "type"), live, liveConsumers,
                msgs, field(json, "messages_ready"), field(json, "messages_unacknowledged"));
        if ("?".equals(msgs)) {
            say(tag, "   （管理端还没有这个队列的统计行，返回体只有 %s）", json);
        }
    }

    /** 打印一个队列的全部绑定（default exchange 的自动绑定就是在这里看到的）。 */
    public static void mgmtBindings(String tag, String queue) {
        String json = mgmt("/queues/%2F/" + enc(queue) + "/bindings?columns=source,destination,routing_key");
        if (json.isEmpty()) {
            say(tag, "管理 API 没取到 %s 的绑定", queue);
            return;
        }
        say(tag, "%s 的绑定: source=\"%s\" destination=%s routing_key=\"%s\"",
                queue, field(json, "source"), field(json, "destination"), field(json, "routing_key"));
    }
}
