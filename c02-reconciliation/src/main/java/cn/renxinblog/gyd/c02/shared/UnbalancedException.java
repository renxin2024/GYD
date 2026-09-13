package cn.renxinblog.gyd.c02.shared;

/**
 * 借贷不平衡异常。
 * 正常运行时应该靠应用层校验拦截；若出现在这里，说明调用方传入了不合法的分录组合。
 */
public class UnbalancedException extends RuntimeException {

    private final long debitSum;
    private final long creditSum;

    public UnbalancedException(long debitSum, long creditSum) {
        super(String.format("借贷不平衡: 借方=%d 分, 贷方=%d 分, 差额=%d 分",
                debitSum, creditSum, Math.abs(debitSum - creditSum)));
        this.debitSum = debitSum;
        this.creditSum = creditSum;
    }

    public long getDebitSum() { return debitSum; }
    public long getCreditSum() { return creditSum; }
}