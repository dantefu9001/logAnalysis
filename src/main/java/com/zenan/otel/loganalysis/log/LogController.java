package com.zenan.otel.loganalysis.log;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LogController {

    private final LogService logService;

    public LogController(LogService logService) {
        this.logService = logService;
    }

    @GetMapping("/view")
    public String publish() {
        logService.viewLog();
        return "success";
    }
}
