package apache.kafka.Kafka_Producer_Practice.dto;

import lombok.Data;

@Data
public class CustomerDTO {
    private int id;
    private String name;
    private String email;
    private String contactNumber;
}
