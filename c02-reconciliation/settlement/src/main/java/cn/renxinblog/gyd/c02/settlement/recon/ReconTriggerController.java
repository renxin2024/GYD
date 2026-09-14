package cn.renxinblog.gyd.c02.settlement.recon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 手动触发对账 —— 实验脚手架。
 *
 * <p>对账 Job 本身是定时任务（{@code schedule-ms} 默认 60 秒，首次延迟 30 秒）。
 * 跑实验时等定时器太慢，也不好在「注入故障 → 立刻对账 → 看结果」之间对齐时间，
 * 所以给一个同步触发入口：调完立即返回，日志里就是这一轮的完整结果。
 *
 * <p>同步执行也方便观察：{@link ReconJob#run()} 跑完才返回，
 * 调用方拿到响应时，这一轮的匹配、分级处理、闭环验收都已经打完日志了。
 */
@RestController
@RequestMapping("/api/recon")
public class ReconTriggerController {

    private static final Logger log = LoggerFactory.getLogger(ReconTriggerController.class);

    private final ReconJob reconJob;

    public ReconTriggerController(ReconJob reconJob) {
        this.reconJob = reconJob;
    }

    /** 立即跑一轮日终对账（阻塞到本轮结束） */
    @PostMapping("/run")
    public Map<String, Object> run() {
        log.info("[recon] 手动触发一轮对账");
        reconJob.run();
        return Map.of("status", "ok", "message", "对账已执行，结果见结算服务日志");
    }
}
