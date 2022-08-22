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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service("logService")
public class LogService {

    private Tracer tracer;

    private static final Logger Logger = LoggerFactory.getLogger(LogService.class);

    private final Pattern pattern = Pattern.compile("\\+\\d\\.\\d+");

    Long timeDiff = 0L;
    Long timeStamp = 0L;
    private String timerCallback = "0x560469906E70";
    private String pingPublish = "ping";
    private String pongPublish = "pong";
    private String pingCallback = "0x5604698F4278";
    private String pongCallback = "0x560B1E381E68";

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

    public void viewLog() {
        try {
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("pingpong.log");
            if (null == inputStream) {
                throw new NullPointerException("文件不存在");
            }
            InputStreamReader read = new InputStreamReader(
                    inputStream);
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt;
            //start the whole chain
            timeStamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
            Span parentSpan = tracer.spanBuilder("publish-subscription").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
            try (Scope scope = parentSpan.makeCurrent()) {

                while ((lineTxt = bufferedReader.readLine()) != null) {
                    //timer callback
                    if (lineTxt.contains("callback_start") && lineTxt.contains(timerCallback)) {
                        Logger.info("timer callback");
                        Span span = tracer.spanBuilder("timer callback").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                        try  {
                            getTimeAdded(lineTxt);
                            while ((lineTxt = bufferedReader.readLine()) != null) {
                                if (lineTxt.contains("callback_end") && lineTxt.contains(timerCallback)) {
                                    getTimeAdded(lineTxt);
                                    span.end(timeStamp, TimeUnit.MICROSECONDS);
                                    break;
                                } else {
                                    //publish
                                    if (Objects.requireNonNull(lineTxt).contains("rclcpp_publish") ) {
                                        getTimeAdded(lineTxt);
                                        lineTxt = bufferedReader.readLine();
                                        //ping publish
                                        if (lineTxt.contains(pingPublish)) {
                                            Logger.info("ping publish");
                                            getTimeAdded(lineTxt);
                                            Span publishSpan = tracer.spanBuilder("ping publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                                            try {
                                                getTimeAdded(bufferedReader.readLine());
                                            } finally {
                                                publishSpan.end(timeStamp, TimeUnit.MICROSECONDS);
                                            }
                                        }
                                        //pong publish
                                        if (lineTxt.contains("rcl_publish") && lineTxt.contains(pongPublish)) {
                                            Logger.info("pong publish");
                                            getTimeAdded(lineTxt);
                                            Span publishSpan = tracer.spanBuilder("pong publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                                            try {
                                                getTimeAdded(bufferedReader.readLine());
                                            } finally {
                                                publishSpan.end(timeStamp, TimeUnit.MICROSECONDS);
                                            }
                                        }
                                    }
                                     else{
                                         getTimeAdded(lineTxt);
                                    }
                                }
                            }
                        } finally {
                            span.end(timeStamp, TimeUnit.MICROSECONDS);
                        }

                    }

                    //callback ping
                    if (Objects.requireNonNull(lineTxt).contains("callback_start") && lineTxt.contains(pingCallback)) {
                        Logger.info("ping callback");
                        Span span = tracer.spanBuilder("callback ping").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                        try  {
                            getTimeAdded(lineTxt);
                            while ((lineTxt = bufferedReader.readLine()) != null) {
                                if (lineTxt.contains("callback_end") && lineTxt.contains(pingCallback)) {
                                    getTimeAdded(lineTxt);
                                    span.end(timeStamp, TimeUnit.MICROSECONDS);
                                    break;
                                } else {
                                    if (Objects.requireNonNull(lineTxt).contains("rclcpp_publish") ) {
                                        getTimeAdded(lineTxt);
                                        lineTxt = bufferedReader.readLine();
                                        //pong publish
                                        if (lineTxt.contains("rcl_publish") && lineTxt.contains(pongPublish)) {
                                            Logger.info("pong publish");
                                            getTimeAdded(lineTxt);
                                            Span publishSpan = tracer.spanBuilder("pong publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                                            try {
                                                getTimeAdded(bufferedReader.readLine());
                                            } finally {
                                                publishSpan.end(timeStamp, TimeUnit.MICROSECONDS);
                                            }
                                        }
                                        if (lineTxt.contains("rcl_publish") && lineTxt.contains(pingPublish)) {
                                            Logger.info("ping publish");
                                            getTimeAdded(lineTxt);
                                            Span publishSpan = tracer.spanBuilder("ping publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                                            try {
                                                getTimeAdded(bufferedReader.readLine());
                                            } finally {
                                                publishSpan.end(timeStamp, TimeUnit.MICROSECONDS);
                                            }
                                        }
                                    }
                                    else{
                                        getTimeAdded(lineTxt);
                                    }                                }
                            }
                        } finally {
                            span.end(timeStamp, TimeUnit.MICROSECONDS);
                        }

                    }
                    //callback pong
                    if (Objects.requireNonNull(lineTxt).contains("callback_start") && lineTxt.contains(pongCallback)) {
                        Logger.info("pong callback");
                        Span span = tracer.spanBuilder("callback pong").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                        try  {
                            getTimeAdded(lineTxt);
                            while ((lineTxt = bufferedReader.readLine()) != null) {
                                if (lineTxt.contains("callback_end") && lineTxt.contains(pongCallback)) {
                                    getTimeAdded(lineTxt);
                                    span.end(timeStamp, TimeUnit.MICROSECONDS);
                                    break;
                                } else {
                                    if (Objects.requireNonNull(lineTxt).contains("rclcpp_publish") ) {
                                        getTimeAdded(lineTxt);
                                        lineTxt = bufferedReader.readLine();
                                        //ping publish
                                        if (lineTxt.contains(pingPublish)) {
                                            Logger.info("ping publish");
                                            getTimeAdded(lineTxt);
                                            Span publishSpan = tracer.spanBuilder("ping publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                                            try  {
                                                getTimeAdded(bufferedReader.readLine());
                                            } finally {
                                                publishSpan.end(timeStamp, TimeUnit.MICROSECONDS);
                                            }
                                        }
                                    }if (Objects.requireNonNull(lineTxt).contains("rclcpp_publish") ) {
                                        getTimeAdded(lineTxt);
                                        lineTxt = bufferedReader.readLine();
                                        //ping publish
                                        if (lineTxt.contains(pingPublish)) {
                                            Logger.info("ping publish");
                                            getTimeAdded(lineTxt);
                                            Span publishSpan = tracer.spanBuilder("ping publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                                            try  {
                                                getTimeAdded(bufferedReader.readLine());
                                            } finally {
                                                publishSpan.end(timeStamp, TimeUnit.MICROSECONDS);
                                            }
                                        }
                                        if (lineTxt.contains(pongPublish)) {
                                            Logger.info("pong publish");
                                            getTimeAdded(lineTxt);
                                            Span publishSpan = tracer.spanBuilder("pong publish").setStartTimestamp(timeStamp, TimeUnit.MICROSECONDS).startSpan();
                                            try  {
                                                getTimeAdded(bufferedReader.readLine());
                                            } finally {
                                                publishSpan.end(timeStamp, TimeUnit.MICROSECONDS);
                                            }
                                        }
                                    }
                                    else{
                                        getTimeAdded(lineTxt);
                                    }                                }
                            }
                        } finally {
                            span.end(timeStamp, TimeUnit.MICROSECONDS);


                        }
                    }

                }
                read.close();
            } finally {
                parentSpan.end(timeStamp, TimeUnit.MICROSECONDS);
            }
        } catch (Exception e) {
            Logger.info("读取文件内容出错");
            e.printStackTrace();
        }
    }


    private void getTimeAdded(String line) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            Float f = Float.parseFloat(matcher.group()) * 1000000000f;
            timeDiff += TimeUnit.NANOSECONDS.toMicros(f.longValue());
            timeStamp += timeDiff < 1 ? 1 : timeDiff;
        }
    }
}
