package com.zenan.otel.loganalysis.log;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.extension.annotations.WithSpan;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

@Service("logService")
public class LogService {

    private Tracer tracer;

    private static final Logger LOGGER = LoggerFactory.getLogger(LogService.class);

    @PostConstruct
    public void init() {

        Resource serviceNameResource = Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "test service"));

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(JaegerGrpcSpanExporter.builder()
                                .setEndpoint("http://localhost:14250").build())
                        .build())
                .setResource(serviceNameResource)
                .build();


        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();

        tracer = openTelemetry
                .getTracer("my-tracer", "0.0.1");
    }

    public void messagePublish() {
        try {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("trace.log.txt");
            if (null == inputStream) {
                throw new NullPointerException("文件不存在");
            }
            InputStreamReader read = new InputStreamReader(
                    inputStream);
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if (lineTxt.contains("chatter")) {
                    break;
                }
            }
            while ((lineTxt = bufferedReader.readLine()) != null) {
                if (lineTxt.contains("rclcpp_publish")) {
                    //开始rclcpp_publish
                    Long timeStamp = System.currentTimeMillis();
                    rclCppPublish(timeStamp, lineTxt, bufferedReader);

                }
            }
            read.close();

        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
    }

//    @WithSpan
    void rclCppPublish(Long timeStamp, String lineTxt, BufferedReader bufferedReader) {
        timeStamp += 1000L;
        Span parentSpan = tracer.spanBuilder("ros2::rclcpp_publish").setStartTimestamp(timeStamp, TimeUnit.MILLISECONDS).startSpan();
        try (Scope scope = parentSpan.makeCurrent()) {
            rclPublish(timeStamp, bufferedReader);
        } finally {
            parentSpan.end(timeStamp + 600L, TimeUnit.MILLISECONDS);
        }
    }


//    @WithSpan
    void rclPublish(Long timeStamp, BufferedReader bufferedReader) {
        timeStamp += 1000L;
        Span childSpan = tracer.spanBuilder("ros2::rcl_publish").setStartTimestamp(timeStamp, TimeUnit.MILLISECONDS).startSpan();
        try (Scope scope = childSpan.makeCurrent()) {
            rmwPublish(timeStamp, bufferedReader);
        } finally {
            childSpan.end(timeStamp + 500, TimeUnit.MILLISECONDS);
        }
    }

//    @WithSpan
    void rmwPublish(long timeStamp, BufferedReader bufferedReader) {
        timeStamp += 1000L;
        Span grandChildSpan = tracer.spanBuilder("ros2::rmw_publish").setStartTimestamp(timeStamp, TimeUnit.MILLISECONDS).startSpan();
        try (Scope scope = grandChildSpan.makeCurrent()) {
            LOGGER.info("Over");
        } finally {
            grandChildSpan.end(timeStamp + 500, TimeUnit.MILLISECONDS);
        }
    }
}
