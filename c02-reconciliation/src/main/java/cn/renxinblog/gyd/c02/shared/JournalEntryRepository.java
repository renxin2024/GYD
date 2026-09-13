package cn.renxinblog.gyd.c02.shared;

import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/**
 * 账务分录 Repository。
 *
 * 对账 Job 用 findByBookedAtBetween 拉取指定时间段内的已入账分录（不含冲正分录），
 * 然后按 tradeId 聚合匹配。
 *
 * 该 Repository 只在银行侧（bank-a / bank-b profile）装配，
 * 结算服务的数据库没有 journal_entry 表。
 */
@Repository
@Profile("bank-a | bank-b")
public interface JournalEntryRepository extends JpaRepository<JournalEntry, Long> {

    /** 按记账时间范围拉取已入账分录（不含冲正分录，冲正分录单独查询） */
    @Query("SELECT j FROM JournalEntry j WHERE j.bookedAt >= :start AND j.bookedAt < :end AND j.reversalOf IS NULL ORDER BY j.bookedAt")
    List<JournalEntry> findOriginalByBookedAtBetween(@Param("start") Instant start, @Param("end") Instant end);

    /**
     * 按记账时间范围拉取「净分录」——原始分录中尚未被冲正的部分。
     * 对账匹配和闭环验收都用这个查询，语义统一：被完整冲正的 journal 不再参与匹配。
     */
    @Query("""
            SELECT j FROM JournalEntry j
            WHERE j.bookedAt >= :start AND j.bookedAt < :end
              AND j.reversalOf IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM JournalEntry r WHERE r.reversalOf = j.entryId
              )
            ORDER BY j.bookedAt
            """)
    List<JournalEntry> findNetByBookedAtBetween(@Param("start") Instant start, @Param("end") Instant end);

    /** 按结算指令 ID 查询（用于幂等检查） */
    boolean existsByTradeId(String tradeId);

    /** 按 entryId 查找 */
    JournalEntry findByEntryId(String entryId);

    /** 按 tradeId 查询该交易的全部已入账分录（含冲正分录，用于完整 journal 冲正与闭环核对） */
    @Query("SELECT j FROM JournalEntry j WHERE j.tradeId = :tradeId ORDER BY j.id")
    List<JournalEntry> findAllByTradeId(@Param("tradeId") String tradeId);

    /** 按 tradeId 查询该交易的已入账分录（不含冲正分录） */
    @Query("SELECT j FROM JournalEntry j WHERE j.tradeId = :tradeId AND j.reversalOf IS NULL")
    List<JournalEntry> findOriginalByTradeId(@Param("tradeId") String tradeId);

    /**
     * 按 tradeId 查询该交易的「净分录」——原始分录中尚未被冲正的那部分。
     *
     * 冲正是追加式的：原始分录（reversalOf IS NULL）永远保留，
     * 冲正分录用 reversalOf 指向被冲正的 entryId。
     * 所以「净分录」= 原始分录中，不存在任何冲正分录指向自己的那些。
     *
     * 闭环验收用这个查询：一个被完整冲正的 journal，净分录应为空；
     * 一个重复入账、只冲正多余那组的 journal，净分录应恰好剩一组。
     */
    @Query("""
            SELECT j FROM JournalEntry j
            WHERE j.tradeId = :tradeId
              AND j.reversalOf IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM JournalEntry r WHERE r.reversalOf = j.entryId
              )
            """)
    List<JournalEntry> findNetByTradeId(@Param("tradeId") String tradeId);
}