package org.kafka.jasik.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

@SpringBootApplication
@Slf4j
public class SpringConsumerApplication {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplication.class);
        application.run(args);
    }

    /**
     * 가장 기본적 리스너 선언
     * ConsumerRecord 파라미터
     *
     * @param record ConsumerRecord<String, String> 메시지키, 메시지값 처리를 할 수 있는 정보들을 담은 컨슈머 레코드 객체
     */
    @KafkaListener(
            topics = "test",
            groupId = "test-group-00"
    )
    public void recordListener(ConsumerRecord<String, String> record) {
        log.info("recordListener : " + record.toString());
    }

    /**
     * 메시지 값을 파라미터로 받음
     * 기본 역직렬화 클래스 StringDeserializer 사용한 경우 String 타입으로 파라미터 받음
     *
     * @param messageValue String
     */
    @KafkaListener(
            topics = "test",
            groupId = "test-group-01"
    )
    public void singleTopicListener(String messageValue) {
        log.info("singleTopicListener : " + messageValue);
    }

    /**
     * properties 로 카프카 컨슈머 옵션 설정
     *
     * @param messageValue
     */
    @KafkaListener(
            topics = "test",
            groupId = "test-group-02",
            properties = {
                    "max.poll.interval.ms:60000",
                    "auto.offset.reset:earliest"
            }
    )
    public void singleTopicWithPropertiesListener(String messageValue) {
        log.info("singleTopicWithPropertiesListener : " + messageValue);
    }

    /**
     * 2개 이상의 컨슈머 스레드를 실행하려면 concurrency 옵션 사용
     *
     * @param messageValue
     */
    @KafkaListener(
            topics = "test",
            groupId = "test-group-03",
            concurrency = "3"
    )
    public void concurrentTopicListener(String messageValue) {
        log.info("concurrentTopicListener : " + messageValue);
    }

    /**
     * 특정 파티션만 구독하고 싶다면 topicPartitions 사용
     * PartitionOffset 어노테이션 사용하여 특정 파티션의 특정 오프셋 지정 가능
     *
     * @param record
     */
    @KafkaListener(
            topicPartitions = {
                    @TopicPartition(topic = "test01", partitions = {"0", "1"}),
                    @TopicPartition(topic = "test02", partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "3"))
            },
            groupId = "test-group-04"
    )
    public void listenSpecificPartition(ConsumerRecord<String, String> record) {
        log.info("listenSpecificPartition : " + record.toString());
    }
}

