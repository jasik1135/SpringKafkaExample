package org.kafka.jasik.producer.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
public class KafkaTemplateConfiguration {
    private final String bootstrapServer;
    private final String acks;

    public KafkaTemplateConfiguration(
            @Value("${spring.kafka.producer.bootstrap-servers}") String bootstrapServer,
            @Value("${spring.kafka.producer.acks}") String acks
    ) {
        this.bootstrapServer = bootstrapServer;
        this.acks = acks;
    }

    @Bean
    public KafkaTemplate<String, String> customKafkaTemplate() {
        Map<String, Object> properties = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, acks
        );

        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(properties);

        return new KafkaTemplate<>(producerFactory);
    }
}
