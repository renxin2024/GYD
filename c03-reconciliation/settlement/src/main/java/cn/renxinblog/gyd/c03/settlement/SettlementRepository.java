package cn.renxinblog.gyd.c03.settlement;

import cn.renxinblog.gyd.c03.shared.clearing.SettlementRecord;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface SettlementRepository extends JpaRepository<SettlementRecord, Long> {

    /** 按结算时间范围拉取已结算记录 */
    @Query("SELECT s FROM SettlementRecord s WHERE s.settledAt >= :start AND s.settledAt < :end AND s.status = 'SETTLED' ORDER BY s.settledAt")
    List<SettlementRecord> findSettledBetween(@Param("start") Instant start, @Param("end") Instant end);

    /** 按 tradeId 查找 */
    SettlementRecord findByTradeId(String tradeId);
}