package com.zenan.otel.loganalysis;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SpringBootTest
class LogAnalysisApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	void testNumber(){
		String line = "[112506.350550095] (+0.000096568) zenan-VirtualBox ros2rmw_take { cpu_id = 1 }, { vtid = 7783, vpid = 7770, procname = talker }, { rmw_subscription_handle = 0x5591E88ECE70, message = 0x7F0186C09E80, source_timestamp = 0, taken = 0 }\n";
		Pattern pattern = Pattern.compile("\\+\\d\\.\\d+");
		Matcher matcher = pattern.matcher(line);
		if (matcher.find()) {
			Float f = Float.parseFloat(matcher.group()) * 1000000000f;
			Long l = f.longValue();
			System.out.println(l);
		}

	}

}
