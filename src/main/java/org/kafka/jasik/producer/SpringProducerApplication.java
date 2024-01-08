package org.kafka.jasik.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@SpringBootApplication
@Slf4j
public class SpringProducerApplication implements CommandLineRunner {
    private static final String INTEGERS_TOPIC_NAME = "integers";
    private static final String CUSTOM_TOPIC_NAME = "custom";

    private final KafkaTemplate<String, String> customKafkaTemplate;

    @Autowired
    public SpringProducerApplication(
            KafkaTemplate<String, String> customKafkaTemplate
    ) {
        this.customKafkaTemplate = customKafkaTemplate;
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        sendIntegers();
        sendCustomTest();

        System.exit(0);
    }

    private void sendIntegers() {

        for (int i = 0; i < 10; i++) {
            customKafkaTemplate.send(INTEGERS_TOPIC_NAME, "" + i);
        }
    }

    private void sendCustomTest() {
        ListenableFuture<SendResult<String, String>> future = customKafkaTemplate.send(CUSTOM_TOPIC_NAME, "test custom");

        future.addCallback(new KafkaSendCallback<>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("send success");
            }

            @Override
            public void onFailure(KafkaProducerException exception) {
                log.warn("send failed");
            }
        });
    }
}
