package com.barriodigital.demo.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @GetMapping("/test-ci")
    public String test() {
        return "PIPELINE_OK_V1";
    }
}