package it.fabricalab.flink.dynamodb.sink;

import java.util.Properties;

public class DynamoDBProducerUtils {
    public static DynamoDBProducerConfiguration getValidatedProducerConfiguration(Properties configProps) {
        return new DynamoDBProducerConfiguration().setRegion("DUMMY_REGION");
    }
}
