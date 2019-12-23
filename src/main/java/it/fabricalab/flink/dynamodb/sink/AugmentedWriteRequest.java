package it.fabricalab.flink.dynamodb.sink;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.io.Serializable;

@AllArgsConstructor
@Setter
@Getter
@ToString
@Accessors(chain = true)
public class AugmentedWriteRequest implements Serializable {
    private String tableName;
    private WriteRequest request;
}
