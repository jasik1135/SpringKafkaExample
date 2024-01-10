package org.kafka.jasik.consumer.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Collection;
import java.util.Map;

@Configuration
public class ListenerContainerConfiguration {
    private final String bootstrapServer;

    public ListenerContainerConfiguration(
            @Value("${spring.kafka.consumer.bootstrap-servers}") String bootstrapServer
    ) {
        this.bootstrapServer = bootstrapServer;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> customContainerFactory() {
        Map<String, Object> properties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(properties);

        ConcurrentKafkaListenerContainerFactory<String, String> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();

        containerFactory.getContainerProperties()
                .setConsumerRebalanceListener(
                        new ConsumerAwareRebalanceListener() {
                            /**
                             * 커밋이 되기 전 리밸런스가 발생했을 경우
                             * @param consumer the consumer.
                             * @param partitions the partitions.
                             */
                            @Override
                            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                            }

                            /**
                             * 커밋이 일어난 이후에 리밸런스가 발생했을 경우
                             * @param consumer the consumer.
                             * @param partitions the partitions.
                             */
                            @Override
                            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                            }

                            @Override
                            public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                            }

                            @Override
                            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                            }
                        }
                );

        containerFactory.setBatchListener(false); // 레코드 리스너 사용을 명시
        containerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD); // 레코드 단위 커밋
        containerFactory.setConsumerFactory(consumerFactory);

        return containerFactory;
    }
}
