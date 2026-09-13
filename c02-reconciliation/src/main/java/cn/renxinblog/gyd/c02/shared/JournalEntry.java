package cn.renxinblog.gyd.c02.shared;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * 账务分录 —— append-only，已入账的行永不修改或删除。
 *
 * 核心约束：
 * <ul>
 *   <li>{@code entry_id} 唯一：每条分录的业务身份，冲正时通过 reversalOf 指向被冲正的原始 entryId</li>
 *   <li>{@code trade_id}：关联的清算指令 ID，对账匹配的核心键</li>
 *   <li>{@code amountCents}：金额，最小货币单位（分），用长整型避免浮点舍入</li>
 *   <li>一笔完整的账务交易（journal entry）通常包含两条分录（一借一贷），
 *       它们共享同一个 tradeId 但各有独立的 entryId</li>
 * </ul>
 */
@Entity
@Table(name = "journal_entry", indexes = {
    @Index(name = "idx_entry_trade", columnList = "trade_id"),
    @Index(name = "idx_entry_booked", columnList = "booked_at")
})
public class JournalEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 业务流水号（唯一）。冲正分录的 entryId 以 REV- 前缀开头方便识别。 */
    @Column(name = "entry_id", nullable = false, unique = true, length = 64)
    private String entryId;

    /** 清算指令 ID：关联结算服务产生的交易标识。多条分录可共享同一 tradeId。 */
    @Column(name = "trade_id", nullable = false, length = 64)
    private String tradeId;

    /** 科目标识（简化：bank_a_cust / bank_a_cbdc / bank_b_cust / bank_b_cbdc） */
    @Column(name = "account_id", nullable = false, length = 32)
    private String accountId;

    /** 借贷方向 */
    @Enumerated(EnumType.STRING)
    @Column(name = "direction", nullable = false, length = 6)
    private Direction direction;

    /** 金额（分） */
    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    /** 业务发生时间（来自结算服务的 settledAt） */
    @Column(name = "trade_time", nullable = false)
    private Instant tradeTime;

    /** 记账时间（数据库写入时自动生成） */
    @Column(name = "booked_at", nullable = false)
    private Instant bookedAt = Instant.now();

    /** 若为冲正分录，指向被冲正的原始 entryId；否则为 null */
    @Column(name = "reversal_of", length = 64)
    private String reversalOf;

    public JournalEntry() {}

    public JournalEntry(String entryId, String tradeId, String accountId,
                        Direction direction, long amountCents, Instant tradeTime,
                        String reversalOf) {
        this.entryId = entryId;
        this.tradeId = tradeId;
        this.accountId = accountId;
        this.direction = direction;
        this.amountCents = amountCents;
        this.tradeTime = tradeTime;
        this.reversalOf = reversalOf;
    }

    // --- getters / setters ---

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getEntryId() { return entryId; }
    public void setEntryId(String entryId) { this.entryId = entryId; }

    public String getTradeId() { return tradeId; }
    public void setTradeId(String tradeId) { this.tradeId = tradeId; }

    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }

    public Direction getDirection() { return direction; }
    public void setDirection(Direction direction) { this.direction = direction; }

    public long getAmountCents() { return amountCents; }
    public void setAmountCents(long amountCents) { this.amountCents = amountCents; }

    public Instant getTradeTime() { return tradeTime; }
    public void setTradeTime(Instant tradeTime) { this.tradeTime = tradeTime; }

    public Instant getBookedAt() { return bookedAt; }
    public void setBookedAt(Instant bookedAt) { this.bookedAt = bookedAt; }

    public String getReversalOf() { return reversalOf; }
    public void setReversalOf(String reversalOf) { this.reversalOf = reversalOf; }
}