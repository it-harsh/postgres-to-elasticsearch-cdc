package com.poc.cdc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class CdcPocApplication {
    public static void main(String[] args) {
        SpringApplication.run(CdcPocApplication.class, args);
    }
}
