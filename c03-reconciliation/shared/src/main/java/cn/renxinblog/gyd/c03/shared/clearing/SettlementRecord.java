package cn.renxinblog.gyd.c03.shared.clearing;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * 清算结果记录 —— 模拟央行结算系统产生的清算数据。
 *
 * 这是对账的「第三方权威数据源」：结算侧说这笔交易该付、该转，它就是事实。
 * 银行的分录以此为基准做匹配和修正。
 */
@Entity
@Table(name = "settlement_record")
public class SettlementRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 清算指令 ID（唯一，对账的核心匹配键） */
    @Column(name = "trade_id", nullable = false, unique = true, length = 64)
    private String tradeId;

    /** 付款银行 */
    @Column(name = "from_bank", nullable = false, length = 16)
    private String fromBank;

    /** 收款银行 */
    @Column(name = "to_bank", nullable = false, length = 16)
    private String toBank;

    /** 金额（分） */
    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    /** 结算完成时间 */
    @Column(name = "settled_at", nullable = false)
    private Instant settledAt;

    /** 结算状态 */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private SettlementStatus status = SettlementStatus.SETTLED;

    public SettlementRecord() {}

    public SettlementRecord(String tradeId, String fromBank, String toBank,
                            long amountCents, Instant settledAt) {
        this.tradeId = tradeId;
        this.fromBank = fromBank;
        this.toBank = toBank;
        this.amountCents = amountCents;
        this.settledAt = settledAt;
        this.status = SettlementStatus.SETTLED;
    }

    // --- getters / setters ---

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getTradeId() { return tradeId; }
    public void setTradeId(String tradeId) { this.tradeId = tradeId; }

    public String getFromBank() { return fromBank; }
    public void setFromBank(String fromBank) { this.fromBank = fromBank; }

    public String getToBank() { return toBank; }
    public void setToBank(String toBank) { this.toBank = toBank; }

    public long getAmountCents() { return amountCents; }
    public void setAmountCents(long amountCents) { this.amountCents = amountCents; }

    public Instant getSettledAt() { return settledAt; }
    public void setSettledAt(Instant settledAt) { this.settledAt = settledAt; }

    public SettlementStatus getStatus() { return status; }
    public void setStatus(SettlementStatus status) { this.status = status; }
}