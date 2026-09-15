package cn.renxinblog.gyd.c03;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

/**
 * 寻址与建连：客户端怎么知道消息该发给谁。
 *
 * <p>这一节的两个点：① 路由是向 NameServer 拉来的（不是配置写死的）；
 * ② 客户端手里的间隔参数决定了「路由变更多久才生效」。
 */
public final class AddressingDemos {

    private AddressingDemos() {
    }

    /** 拉一次路由并打印地址表。对应文章第一节。 */
    static void run() throws Exception {
        DemoRunner.head("route");
        DefaultMQProducer probe = DemoRunner.producer("gyd_c03_route_probe");
        try {
            DemoRunner.say("route", "namesrvAddr              = %s", probe.getNamesrvAddr());
            DemoRunner.say("route", "pollNameServerInterval   = %d ms   <- 路由刷新间隔", probe.getPollNameServerInterval());
            DemoRunner.say("route", "heartbeatBrokerInterval  = %d ms   <- 向 broker 心跳间隔", probe.getHeartbeatBrokerInterval());
            DemoRunner.say("route", "persistConsumerOffset    = %d ms   <- 位点持久化间隔（消费端用）", probe.getPersistConsumerOffsetInterval());

            MQClientInstance instance = probe.getDefaultMQProducerImpl().getMqClientFactory();
            DemoRunner.say("route", "clientId                 = %s", instance.getClientId());

            MQClientAPIImpl api = instance.getMQClientAPIImpl();
            TopicRouteData route = api.getTopicRouteInfoFromNameServer(DemoRunner.TOPIC_SEND, 3000L);
            if (route == null) {
                DemoRunner.say("route", "主题 %s 不存在（NameServer 上没有它的路由）", DemoRunner.TOPIC_SEND);
                return;
            }
            for (BrokerData broker : route.getBrokerDatas()) {
                DemoRunner.say("route", "broker %s 地址表 = %s", broker.getBrokerName(), broker.getBrokerAddrs());
            }
            for (QueueData queue : route.getQueueDatas()) {
                DemoRunner.say("route", "队列 %s read=%d write=%d perm=%s",
                        queue.getBrokerName(), queue.getReadQueueNums(), queue.getWriteQueueNums(), queue.getPerm());
            }
            DemoRunner.say("route", "注意：地址表里是 broker 自己通告的地址（brokerIP1），不是 NameServer 的地址");
        } finally {
            probe.shutdown();
        }
    }

    /**
     * 自动建 Topic 的对照实验。对应文章第九节。
     *
     * <p>先用一个从未显式建过的主题名拉路由，再发一条消息（broker 自动建），
     * 然后再拉一次路由看队列数——这样拿到的是「自动建出来的队列数」，不是推断值。
     */
    static void autoCreateProbe() throws Exception {
        DemoRunner.head("autocreate");
        String topic = "gyd-c03-autocreate-probe";
        DefaultMQProducer producer = DemoRunner.producer("gyd_c03_autocreate_probe");
        try {
            MQClientAPIImpl api = producer.getDefaultMQProducerImpl().getMqClientFactory().getMQClientAPIImpl();

            DemoRunner.say("autocreate", "发送前：拉 %s 的路由", topic);
            try {
                TopicRouteData before = api.getTopicRouteInfoFromNameServer(topic, 3000L);
                DemoRunner.say("autocreate", "  路由已存在 = %s（说明之前建过，本次实验需要换个主题名）", before != null);
            } catch (MQClientException e) {
                DemoRunner.say("autocreate", "  抛 MQClientException CODE=%d DESC=%s",
                        e.getResponseCode(), e.getErrorMessage());
                DemoRunner.say("autocreate", "  即：主题不存在时不是返回 null，而是抛 TOPIC_NOT_EXIST(%d)", ResponseCode.TOPIC_NOT_EXIST);
            }

            SendResult result = producer.send(new Message(topic, DemoRunner.utf8("autocreate-probe")));
            DemoRunner.say("autocreate", "自动建：sendStatus=%s queueId=%d", result.getSendStatus(), result.getMessageQueue().getQueueId());

            DemoRunner.sleep(1_000);
            TopicRouteData after = api.getTopicRouteInfoFromNameServer(topic, 3000L);
            if (after == null) {
                DemoRunner.say("autocreate", "仍拿不到路由（broker 还没上报）");
                return;
            }
            for (QueueData queue : after.getQueueDatas()) {
                DemoRunner.say("autocreate", "自动建出的队列 %s read=%d write=%d",
                        queue.getBrokerName(), queue.getReadQueueNums(), queue.getWriteQueueNums());
            }
            DemoRunner.say("autocreate", "对照：用 mqadmin updateTopic -w 4 显式建的 gyd-c03-send 是 write=4");
        } finally {
            producer.shutdown();
        }
    }
}
