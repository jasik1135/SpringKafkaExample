package org.kafka.jasik.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringConsumerApplication {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplication.class);
        application.run(args);
    }
}

