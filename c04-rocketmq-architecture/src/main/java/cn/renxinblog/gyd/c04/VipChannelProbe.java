package cn.renxinblog.gyd.c04;

import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

/**
 * VIP 通道对照探针：**客户端最终把 TCP 连到 listenPort，还是 listenPort-2？**
 *
 * <p>库里的机制（4.9.7 实测 + 字节码核对）：
 * <ul>
 *   <li>变换只有一处实现，{@link MixAll#brokerVIPChannel(boolean, String)}：第一个参数为真时
 *       把地址端口减 2，为假时原样返回。</li>
 *   <li>{@code DefaultMQProducerImpl.sendKernelImpl} 里传的是
 *       {@code defaultMQProducer.isSendMessageWithVIPChannel()}；
 *       {@code MQClientAPIImpl} 里有 48 处传的是 {@code clientConfig.isVipChannelEnabled()}，
 *       且这 48 处<b>全是运维/管理类接口</b>（createTopic、getBrokerRuntimeInfo、
 *       queryConsumerOffset、lockBatchMQ…），<b>不含</b> {@code sendMessage} 与
 *       {@code sendHearbeat}——所以心跳永远走 listenPort。</li>
 *   <li>两个方法名不同，但 {@code DefaultMQProducer.isSendMessageWithVIPChannel()} 的方法体
 *       就是 {@code return isVipChannelEnabled();}，{@code setSendMessageWithVIPChannel}
 *       同理转发到 {@code setVipChannelEnabled}——<b>背后是同一个字段</b>。</li>
 *   <li>该字段在 {@code ClientConfig} 构造器里被初始化为
 *       {@code Boolean.parseBoolean(System.getProperty("com.rocketmq.sendMessageWithVIPChannel", "false"))}，
 *       缺省值是 <b>false</b>。</li>
 * </ul>
 *
 * <p>结论是反直觉的：「客户端默认连 listenPort-2」并不成立。默认连 listenPort；
 * 只有显式把开关打开，<b>发送</b>才会另开一条到 listenPort-2 的连接。
 *
 * <p>本探针的做法是：打印开关默认值 → 算出「实际会连的地址」→ <b>持有连接若干秒</b>
 * （让外部用 {@code lsof -a -p <pid>} 按进程归属，而不是靠猜）→ 再发一条消息看结果。
 *
 * <p>用法：
 * <pre>
 *   run --args="vip"               # 不改任何开关，观察库默认行为
 *   run --args="vip on"            # 显式 setVipChannelEnabled(true)
 *   run --args="vip on 8 20"       # 发送前持有 8 秒、发送后持有 20 秒，供外部两次归属
 * </pre>
 *
 * <p>为什么要有「发送前 / 发送后」两段持有：启动阶段建立的那条连接是心跳，而心跳不走
 * VIP 变换。只看启动后的连接，会误判成「VIP 开关没生效」。
 *
 * <p>为什么这个类没有「裸 TCP 连通性探测」：写过一版用 {@code Socket.connect} 探端口，
 * 结果在受限沙箱里对非回环地址一律返回「连接成功」（连 1 端口都成功），
 * 量具本身不可信。端口是否可达交给 {@code nc -z} 或让客户端自己报错，
 * 不在这里造一个看起来像证据的东西。
 */
public final class VipChannelProbe {

    private VipChannelProbe() {
    }

    public static void run(String[] argv) throws Exception {
        // argv[0] 是 "vip"，argv[1] 可选 on/off，argv[2]/argv[3] 可选发送前后的持有秒数
        String flagArg = argv.length > 1 ? argv[1] : "default";
        int holdBefore = argv.length > 2 ? Integer.parseInt(argv[2]) : 0;
        int holdAfter = argv.length > 3 ? Integer.parseInt(argv[3]) : 0;

        DemoRunner.head("vip-channel  pid=" + ProcessHandle.current().pid());

        DefaultMQProducer producer = new DefaultMQProducer("gyd-c03-vip-probe");
        producer.setNamesrvAddr(DemoRunner.namesrvAddr());
        // 单次尝试：不让重试把「一次连接失败」糊成一段噪声
        producer.setSendMsgTimeout(3000);
        producer.setRetryTimesWhenSendFailed(0);

        DemoRunner.say("vip", "库默认 vipChannelEnabled = %s", producer.isVipChannelEnabled());
        DemoRunner.say("vip", "库默认 sendMessageWithVIPChannel = %s（只是 isVipChannelEnabled 的转发，同一字段）",
                producer.isSendMessageWithVIPChannel());

        if ("on".equals(flagArg)) {
            producer.setVipChannelEnabled(true);
        } else if ("off".equals(flagArg)) {
            producer.setVipChannelEnabled(false);
        }
        boolean vip = producer.isVipChannelEnabled();
        DemoRunner.say("vip", "本次生效 vipChannelEnabled = %s（参数 %s）", vip, flagArg);
        DemoRunner.say("vip", "两个方法读到的值是否一致 = %s",
                vip == producer.isSendMessageWithVIPChannel());

        producer.start();

        // 从 NameServer 拉真实注册地址，而不是把地址写死在代码里
        MQClientInstance factory = producer.getDefaultMQProducerImpl().getmQClientFactory();
        MQClientAPIImpl api = factory.getMQClientAPIImpl();
        TopicRouteData route = api.getTopicRouteInfoFromNameServer(DemoRunner.TOPIC_SEND, 3000);
        BrokerData bd = route.getBrokerDatas().get(0);
        String registered = bd.getBrokerAddrs().get(MixAll.MASTER_ID);

        String willConnect = MixAll.brokerVIPChannel(vip, registered);
        DemoRunner.say("vip", "broker 在 NameServer 注册的地址 = %s", registered);
        DemoRunner.say("vip", "套用 brokerVIPChannel(%s, ...) 之后 = %s", vip, willConnect);
        DemoRunner.say("vip", "=> 预期发送时这个进程的 ESTABLISHED 会落在端口 %s",
                willConnect.substring(willConnect.lastIndexOf(':') + 1));

        if (holdBefore > 0) {
            DemoRunner.say("vip", "【发送前】持有 %d 秒（pid=%d），这一段的连接来自心跳/路由，外部可 lsof 归属",
                    holdBefore, ProcessHandle.current().pid());
            System.out.flush();
            DemoRunner.sleep(holdBefore * 1000L);
        }

        Message msg = new Message(DemoRunner.TOPIC_SEND, "VIP", DemoRunner.utf8("vip-probe " + flagArg));
        try {
            SendResult result = producer.send(msg);
            DemoRunner.say("vip", "发送结果 = %s, queue=%s", result.getSendStatus(), result.getMessageQueue());
        } catch (Throwable t) {
            DemoRunner.say("vip", "发送抛异常 = %s", t.getClass().getName());
            DemoRunner.say("vip", "异常消息 = %s", t.getMessage());
            Throwable c = t.getCause();
            int depth = 0;
            while (c != null && depth++ < 4) {
                DemoRunner.say("vip", "  cause[%d] = %s: %s", depth, c.getClass().getName(), c.getMessage());
                c = c.getCause();
            }
        }

        // 发送之后仍持有连接：这一段的连接才是发送路径落下的，必须单独抓
        if (holdAfter > 0) {
            System.out.flush();
            DemoRunner.say("vip", "【发送后】持有 %d 秒（pid=%d）", holdAfter, ProcessHandle.current().pid());
            System.out.flush();
            DemoRunner.sleep(holdAfter * 1000L);
        }
        producer.shutdown();
    }
}
