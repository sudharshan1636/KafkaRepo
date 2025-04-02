package apache.kafka.Kafka_Producer_Practice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic createTopic(){
        return new NewTopic("Topic3",5, (short) 1);
    }
}
