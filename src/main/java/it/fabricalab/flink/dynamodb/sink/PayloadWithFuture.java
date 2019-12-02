package it.fabricalab.flink.dynamodb.sink;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.SettableFuture;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Setter
@Getter
@Accessors(chain = true)
public class PayloadWithFuture implements Serializable {
    private Map<String, List<WriteRequest>> payload;
    private SettableFuture<WriteItemResult> future;
}
