package it.fabricalab.flink.dynamodb.sink;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
@NoArgsConstructor
public class DynamoDBProducerConfiguration {
    private String region;
}
