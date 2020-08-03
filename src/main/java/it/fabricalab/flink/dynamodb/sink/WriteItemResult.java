package it.fabricalab.flink.dynamodb.sink;

import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Serializable;

@AllArgsConstructor
@Setter
@Getter
@Accessors(chain = true)
public class WriteItemResult implements Serializable {
    private boolean successful;
    private BatchWriteItemResult result;
}
