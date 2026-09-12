package cn.renxinblog.gyd.c01.message;

/**
 * 订单创建事件。
 *
 * 用 record 承载消息体：字段不可变，Jackson 3 可直接反序列化，
 * 不需要额外的无参构造器和 setter。
 */
public record OrderMessage(String orderId, String userId, long amount, String createdAt) {
}
