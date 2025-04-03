package apache.kafka.Kafka_Producer_Practice.service;

import apache.kafka.Kafka_Producer_Practice.dto.CustomerDTO;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public KafkaMessagePublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send("Topic2", message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata metadata = result.getRecordMetadata();
                System.out.printf("""
                    Message sent successfully!
                    Topic: %s
                    Partition: %d
                    Offset: %d
                    Message: %s
                    %n""",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        message);
            } else {
                System.err.printf("""
                    Failed to send message: %s
                    Error: %s
                    %n""",
                        message,
                        ex.getMessage());
            }
        });
    }

    public void sendEventToTopic(CustomerDTO customer) {
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send("Customer1", customer);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata metadata = result.getRecordMetadata();
                System.out.printf("""
                    Message sent successfully!
                    Topic: %s
                    Partition: %d
                    Offset: %d
                    Message: %s
                    %n""",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        customer);
            } else {
                System.err.printf("""
                    Failed to send message: %s
                    Error: %s
                    %n""",
                        customer,
                        ex.getMessage());
            }
        });
    }
}