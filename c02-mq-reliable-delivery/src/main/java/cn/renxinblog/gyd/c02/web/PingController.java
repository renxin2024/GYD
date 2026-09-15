package cn.renxinblog.gyd.c02.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 框架自检端点，用来确认应用能正常启动并对外提供 HTTP 服务。
 *
 * 真正触发消息发送的下单入口见 {@link OrderController}。
 */
@RestController
@RequestMapping("/gyd/c02")
public class PingController {

    @GetMapping("/ping")
    public Map<String, String> ping() {
        return Map.of("status", "ok");
    }
}
