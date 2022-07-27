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
import java.util.ArrayList;
import java.util.List;
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
        Span parentSpan = tracer.spanBuilder("rclcpp_publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
        LOGGER.info(lineTxt);
        try (Scope scope = parentSpan.makeCurrent()) {
            rclPublish(bufferedReader);
        } finally {
            parentSpan.end(timeStamp, TimeUnit.MICROSECONDS);
        }
    }


    //    @WithSpan
    void rclPublish(BufferedReader bufferedReader) {
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
            timeStamp += timeDiff < 1 ? 1 : timeDiff;
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
            timeStamp += timeDiff < 1 ? 1 : timeDiff;
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
            grandChildSpan.end(timeStamp, TimeUnit.MICROSECONDS);
        }
    }

    public void subscriptionCallbacks() {
        timeStamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        Span span = tracer.spanBuilder("subscription-callbacks").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
        try(Scope scope = span.makeCurrent()) {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("trace.log.txt");
            if (null == inputStream) {
                throw new NullPointerException("文件不存在");
            }
            InputStreamReader read = new InputStreamReader(
                    inputStream);
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;

            while ((lineTxt = bufferedReader.readLine()) != null) {
                if (lineTxt.contains("rclcpp_executor_execute")) {
                    //开始rclcpp_publish
                    String nextLine = bufferedReader.readLine();
                    if (nextLine.contains("ros2rmw_take")) {
                        List<String> array = new ArrayList<>();
                        array.add(nextLine);
                        for (int i = 0; i < 5; i++) {
                            array.add(bufferedReader.readLine());
                        }
                        timeStamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                        rclCppExecutorExecute(lineTxt, array);
                        timeStamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
                        callbacks(array);
                    }

                }
            }
            read.close();

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }finally {
            span.end();
        }
    }

    private void rclCppExecutorExecute(String line, List<String> lines) {
        timeDiff = 0L;
        Span parentSpan = tracer.spanBuilder("rclcpp_executor_execute").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
        LOGGER.info(line);
        try (Scope scope = parentSpan.makeCurrent()) {
            rclCppTake(lines.get(lines.size() - 4), lines);
        } finally {
            parentSpan.end(timeStamp, TimeUnit.MICROSECONDS);
        }
    }

    private void rclCppTake(String line, List<String> lines) {
        LOGGER.info(line);
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            Float f = Float.parseFloat(matcher.group()) * 1000000000f;
            timeDiff += TimeUnit.NANOSECONDS.toMicros(f.longValue());
            timeStamp += timeDiff < 1 ? 1 : timeDiff;
        }
        Span childSpan = tracer.spanBuilder("rcl_cpp_take").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
        try (Scope scope = childSpan.makeCurrent()) {
            rclTake(lines.get(lines.size() - 5), lines);
        } finally {
            childSpan.end(timeStamp, TimeUnit.MICROSECONDS);
        }
    }

    private void rclTake(String line, List<String> lines) {
        LOGGER.info(line);
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            Float f = Float.parseFloat(matcher.group()) * 1000000000f;
            timeDiff += TimeUnit.NANOSECONDS.toMicros(f.longValue());
            timeStamp += timeDiff < 1 ? 1 : timeDiff;
        }
        Span childSpan = tracer.spanBuilder("rcl_cpp_take").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
        try (Scope scope = childSpan.makeCurrent()) {
            rwmTake(lines.get(lines.size() - 6), lines);
        } finally {
            childSpan.end(timeStamp, TimeUnit.MICROSECONDS);
        }
    }

    private void rwmTake(String line, List<String> lines) {
        LOGGER.info(line);
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            Float f = Float.parseFloat(matcher.group()) * 1000000000f;
            timeDiff += TimeUnit.NANOSECONDS.toMicros(f.longValue());
            timeStamp += timeDiff < 1 ? 1 : timeDiff;
        }
        Span grandChildSpan = tracer.spanBuilder("rmw_take").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();

        try (Scope scope = grandChildSpan.makeCurrent()) {
            line = lines.get(lines.size() - 3);
            matcher = pattern.matcher(line);
            if (matcher.find()) {
                Float f = Float.parseFloat(matcher.group()) * 1000000000f;
                timeDiff += TimeUnit.NANOSECONDS.toMicros(f.longValue());
                timeStamp += timeDiff < 1 ? 1 : timeDiff;
            }
            LOGGER.info(line);
        } finally {
            grandChildSpan.end(timeStamp, TimeUnit.MICROSECONDS);
        }
    }

    private void callbacks(List<String> lines) {
        String line;
        Span span = tracer.spanBuilder("ros2callback_start").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
        try (Scope scope = span.makeCurrent()) {
            line = lines.get(lines.size() - 2);
            Matcher matcher = pattern.matcher(line);

            if (matcher.find()) {
                Float f = Float.parseFloat(matcher.group()) * 1000000000f;
                timeDiff += TimeUnit.NANOSECONDS.toMicros(f.longValue());
                timeStamp += timeDiff < 1 ? 1 : timeDiff;
            }
            LOGGER.info(line);
        } finally {
            span.end(timeStamp, TimeUnit.MICROSECONDS);
        }

        Span endSpan = tracer.spanBuilder("ros2callback_end").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
        try (Scope scope = endSpan.makeCurrent()) {
            line = lines.get(lines.size() - 1);
            Matcher matcher = pattern.matcher(line);

            if (matcher.find()) {
                Float f = Float.parseFloat(matcher.group()) * 1000000000f;
                timeDiff += TimeUnit.NANOSECONDS.toMicros(f.longValue());
                timeStamp += timeDiff < 1 ? 1 : timeDiff;
            }
            LOGGER.info(line);
        } finally {
            endSpan.end(timeStamp, TimeUnit.MICROSECONDS);
        }
    }
}
