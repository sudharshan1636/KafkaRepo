package apache.kafka.Kafka_Producer_Practice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic createTopic(){
        return new NewTopic("Customer1",3, (short) 1);
    }
}
