package com.sfc.aiassistant.controller;

import com.sfc.aiassistant.model.ChatRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * AI 助手对话接口控制器。
 * 提供基于 SSE（Server-Sent Events）的流式对话能力。
 */
@Slf4j
@RestController
@RequestMapping("/api/ai-assistant")
public class AiAssistantController {

    /**
     * SSE 逐字发送线程池。
     * <p>
     * 采用 CallerRunsPolicy 拒绝策略：当线程池饱和时由调用线程执行，
     * 避免任务被静默丢弃导致 emitter 干挂、客户端收不到任何数据。
     * 每个连接会长时间占用线程（逐字发送），因此队列不宜过大，
     * 且线程存活时保持核心线程以应对持续请求。
     */
    private final ExecutorService sseExecutor = new ThreadPoolExecutor(
            4,
            32,
            60, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            r -> {
                Thread t = new Thread(r, "ai-assistant-sse");
                t.setDaemon(true);
                return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
    );

    /**
     * 流式对话接口。
     * 逐字返回固定的 Markdown 演示内容，模拟大模型流式输出效果，结束时发送 [DONE] 标记。
     *
     * @param request 对话请求体，包含用户输入的消息
     * @return SSE 发射器，持续推送消息，结束时推送 [DONE]
     */
    @PostMapping(value = "/chat", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter chat(@RequestBody ChatRequest request, HttpServletResponse response) {
        // 禁用 Nginx/反向代理对响应的缓冲，确保 SSE 事件能即时推送到客户端
        response.setHeader("X-Accel-Buffering", "no");
        // 超时时间需大于内容发送总时长（约 reply.length * 间隔），留足余量
        SseEmitter emitter = new SseEmitter(120000L);

        // 固定返回的 markdown 内容，逐字发送模拟流式效果
        String reply = "##   👋 你好，我是咸鱼云 AI 助手！\n\n"
                + "很高兴为你服务！我目前处于**内测阶段**，这是一个流式返回的演示消息。\n\n"
                + "### 🚀 我能做什么？\n\n"
                + "- 📂 **文件管理**：帮你查找、整理网盘中的文件\n"
                + "- 🔍 **智能搜索**：通过自然语言快速定位你想要的内容\n"
                + "- 📊 **信息汇总**：对文档内容进行摘要和分析\n"
                + "- 🛠️ **快捷操作**：一键执行常用任务\n\n"
                + "### 📌 使用提示\n\n"
                + "> 当前为演示版本，后端返回的是固定 Markdown 内容。\n"
                + "> 正式版将接入 AI 大模型，支持真正的对话交互。\n\n"
                + "你可以尝试输入任意文字，我会逐字打印这段 Markdown，\n"
                + "前端使用 **markdown-it** 进行渲染，支持标题、列表、引用等格式。\n\n"
                + "---\n\n"
                + "😊 敬请期待更多功能上线！";

        // emitter 结束/超时/异常时的清理回调，避免线程泄漏
        emitter.onCompletion(() -> log.info("AI 助手 SSE 连接结束"));
        emitter.onTimeout(() -> {
            log.info("AI 助手 SSE 连接超时");
            emitter.complete();
        });
        emitter.onError(e -> {
            log.error("AI 助手 SSE 连接异常 -> {}", e.getMessage(), e);
            emitter.complete();
        });

        sseExecutor.execute(() -> {
            try {
                // 逐字符发送（按 Unicode Code Point），产生打字机效果。
                // 注意：SSE 协议以 \n 为行分隔符，data 中不能出现原始 \n 字符。
                // 因此将每个码点以十进制数字的形式发送，前端用 String.fromCodePoint() 还原。
                reply.codePoints().forEach(cp -> {
                    try {
                        emitter.send(String.valueOf(cp), MediaType.TEXT_PLAIN);
                        Thread.sleep(20);
                    } catch (IOException ioe) {
                        throw new CompletionException(ioe);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(ie);
                    }
                });
                emitter.send("[DONE]", MediaType.TEXT_PLAIN);
                emitter.complete();
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });

        return emitter;
    }
}
