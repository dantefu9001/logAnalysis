package com.zenan.otel.loganalysis.log;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service("logService")
public class LogService {

    private Tracer tracer;

    private static final Logger LOGGER = LoggerFactory.getLogger(LogService.class);

    private final Pattern pattern = Pattern.compile("\\+\\d\\.\\d+");

    Long timeDiff = 0L;
    Long timeStamp = 0l;

    @PostConstruct
    public void init() {

        Resource serviceNameResource = Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "ros2 tracing"));

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
                    Date date = new Date();
                    timeStamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                    rclCppPublish(lineTxt, bufferedReader);

                }
            }
            read.close();

        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
    }

    //    @WithSpan
    void rclCppPublish(String lineTxt, BufferedReader bufferedReader) throws IOException {
        timeDiff = 0L;
        Long elapsedTime = 0l;
        Span parentSpan = tracer.spanBuilder("rclcpp_publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
        LOGGER.info(lineTxt);
        try (Scope scope = parentSpan.makeCurrent()) {
            rclPublish(bufferedReader);
        } finally {
            parentSpan.end(timeStamp, TimeUnit.MICROSECONDS);
        }
    }


    //    @WithSpan
    void rclPublish( BufferedReader bufferedReader) {
        String line;
        try {
            line = bufferedReader.readLine();
            LOGGER.info(line);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            Float f = Float.parseFloat(matcher.group()) * 1000000000f;
            timeDiff += TimeUnit.NANOSECONDS.toMicros(f.longValue());
            timeStamp += timeDiff <1?1: timeDiff;
        }
        Span childSpan = tracer.spanBuilder("rcl_publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
        try (Scope scope = childSpan.makeCurrent()) {
            rmwPublish(bufferedReader);
        } finally {
            childSpan.end(timeStamp, TimeUnit.MICROSECONDS);
        }
    }

    //    @WithSpan
    void rmwPublish(BufferedReader bufferedReader) {
        String line = null;
        Long elapsedTime = 0L;
        try {
            line = bufferedReader.readLine();
            LOGGER.info(line);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            Float f = Float.parseFloat(matcher.group()) * 1000000000f;
            timeDiff += TimeUnit.NANOSECONDS.toMicros(f.longValue());
            timeStamp += timeDiff <1?1: timeDiff;
        }
        Span grandChildSpan = tracer.spanBuilder("rmw_publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();

        try (Scope scope = grandChildSpan.makeCurrent()) {
            line = bufferedReader.readLine();
            matcher = pattern.matcher(line);
            if (matcher.find()) {
                Float f = Float.parseFloat(matcher.group()) * 1000000000f;
                timeStamp += TimeUnit.NANOSECONDS.toMicros(f.longValue());
            }
            LOGGER.info(line);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            grandChildSpan.end(timeStamp , TimeUnit.MICROSECONDS);
        }
    }
}
