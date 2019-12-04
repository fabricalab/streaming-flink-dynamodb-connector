package it.fabricalab;

import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.ListenableFuture;
import it.fabricalab.flink.dynamodb.sink.FlinkDynamoDBProducer;
import it.fabricalab.flink.dynamodb.sink.AugmentedWriteRequest;
import it.fabricalab.flink.dynamodb.sink.DynamoDBProducer;
import it.fabricalab.flink.dynamodb.sink.DynamoDBProducerConfiguration;
import it.fabricalab.flink.dynamodb.sink.WriteItemResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DynamoDBProducerTest {

    private static final FlinkDynamoDBProducer.Client
            dummyClient = new FlinkDynamoDBProducer.Client() {
        @Override
        public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequestx) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return new BatchWriteItemResult();
        }
    };


    @Test
    void testWithoutSpiller() {

        DynamoDBProducer producer =
                new DynamoDBProducer(new DynamoDBProducerConfiguration(), dummyClient, () -> {
                });

        assertEquals(0, producer.getOutstandingRecordsCount());

        producer.addUserRecord(new AugmentedWriteRequest("XX", new WriteRequest()));

        assertEquals(1, producer.getOutstandingRecordsCount());

        for (int i = 0; i < 25; i++)
            producer.addUserRecord(new AugmentedWriteRequest("YY", new WriteRequest()));


        //all records still in
        assertEquals(26, producer.getOutstandingRecordsCount());

        //only one in the currentmap
        assertEquals(1,
                producer.getCurrentlyUnderConstruction().values().stream()
                        .collect(Collectors.summingInt(l -> l.size())));

        //a map in the queue
        assertEquals(1, producer.getQueue().size());

        //the map containing all the remaining
        assertEquals(25, producer.getQueue().poll().getPayload().values().stream()
                .collect(Collectors.summingInt(l -> l.size())));

    }

    @Test
    void testWithSpiller() throws InterruptedException, ExecutionException, TimeoutException {

        DynamoDBProducer producer =
                new DynamoDBProducer(new DynamoDBProducerConfiguration(), dummyClient);

        List<ListenableFuture> futures = new ArrayList<>();


        ListenableFuture<WriteItemResult> f;
        for (int i = 0; i < 25; i++) {
            f = producer.addUserRecord(new AugmentedWriteRequest("YY", new WriteRequest()));
            futures.add(f);
            assertEquals(0, producer.getSpillerCycles());
        }

        f = producer.addUserRecord(new AugmentedWriteRequest("YY", new WriteRequest()));
        futures.add(f);

        //now it will fire
        //wait for that
        futures.get(0).get(1000, TimeUnit.MILLISECONDS); //should resolve and fast

        assertEquals(1, producer.getSpillerCycles());

        //spiller consumer a map of 25
        assertEquals(1, producer.getOutstandingRecordsCount());

        //one in the currentmap
        assertEquals(1,
                producer.getCurrentlyUnderConstruction().values().stream()
                        .collect(Collectors.summingInt(l -> l.size())));

        //no map in the queue
        assertEquals(0, producer.getQueue().size());


        //all the futures are done bit the last
        assertFalse(futures.get(futures.size() - 1).isDone());

        for (int i = 0; i < futures.size() - 1; i++)
            assertTrue(futures.get(i).isDone());
    }


    @Test
    void flush() {
        DynamoDBProducer producer =
                new DynamoDBProducer(new DynamoDBProducerConfiguration(), dummyClient, () -> {
                });

        for (int i = 0; i < 10; i++)
            producer.addUserRecord(new AugmentedWriteRequest("YY", new WriteRequest()));
        //all records  in
        assertEquals(10, producer.getOutstandingRecordsCount());

        //all in the currentmap
        assertEquals(10,
                producer.getCurrentlyUnderConstruction().values().stream()
                        .collect(Collectors.summingInt(l -> l.size())));

        //nothing in the queue
        assertEquals(0, producer.getQueue().size());


        //move to queue
        producer.flush();

        //zero in the currentmap
        assertEquals(0,
                producer.getCurrentlyUnderConstruction().values().stream()
                        .collect(Collectors.summingInt(l -> l.size())));

        //one in the queue
        assertEquals(1, producer.getQueue().size());


        //the map in the queue containing all the records
        assertEquals(10, producer.getQueue().poll().getPayload().values().stream()
                .collect(Collectors.summingInt(l -> l.size())));


    }

    @Test
    void testFlushAndWait() throws InterruptedException, ExecutionException, TimeoutException {
        DynamoDBProducer producer =
                new DynamoDBProducer(new DynamoDBProducerConfiguration(), dummyClient);

        List<ListenableFuture> futures = new ArrayList<>();


        ListenableFuture<WriteItemResult> f;
        for (int i = 0; i < 25; i++) {
            f = producer.addUserRecord(new AugmentedWriteRequest("YY", new WriteRequest()));
            futures.add(f);
            assertEquals(0, producer.getSpillerCycles());
        }

        f = producer.addUserRecord(new AugmentedWriteRequest("YY", new WriteRequest()));
        futures.add(f);

        //now it will fire
        //wait for that
        futures.get(0).get(1000, TimeUnit.MILLISECONDS); //should resolve and fast

        assertEquals(1, producer.getSpillerCycles());

        //spiller consumer a map of 25
        assertEquals(1, producer.getOutstandingRecordsCount());

        //one in the currentmap
        assertEquals(1,
                producer.getCurrentlyUnderConstruction().values().stream()
                        .collect(Collectors.summingInt(l -> l.size())));

        //no map in the queue
        assertEquals(0, producer.getQueue().size());


        //all the futures are done bit the last
        assertFalse(futures.get(futures.size() - 1).isDone());

        for (int i = 0; i < futures.size() - 1; i++)
            assertTrue(futures.get(i).isDone());

        producer.flushAndWait();

        //and now all the futures should be done
        assertTrue(futures.get(futures.size() - 1).isDone());

    }

    @Test
    void testDestroy() {

        DynamoDBProducer producer =
                new DynamoDBProducer(new DynamoDBProducerConfiguration(), dummyClient);

        assertFalse(producer.getSpillerExecutor().isShutdown());

        producer.destroy();

        assertTrue(producer.getSpillerExecutor().isShutdown());


    }

    @Test
    void testFlushEmpty() {
        DynamoDBProducer producer =
                new DynamoDBProducer(new DynamoDBProducerConfiguration(), dummyClient, () -> {
                });

        assertEquals(0, producer.getCurrentlyUnderConstruction().size());
        producer.flush();

        assertEquals(0, producer.getCurrentlyUnderConstruction().size());
        producer.flush();

        assertEquals(0, producer.getCurrentlyUnderConstruction().size());

    }

    @Test
    void testSpillerRetry() throws ExecutionException, InterruptedException {

        ArrayList<WriteRequest> consumed = new ArrayList<>();

        DynamoDBProducer producer =
                new DynamoDBProducer(new DynamoDBProducerConfiguration(), new FlinkDynamoDBProducer.Client() {
                    //this client consumes only a few elements in each request
                    //counts how many requests are done
                    @Override
                    public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest) {

                        Map<String, List<WriteRequest>> tbd = batchWriteItemRequest.getRequestItems();

                        System.err.println("Incoming payload size: " + tbd.values().stream().collect(Collectors.summingInt(l -> l.size())));
                        System.err.println("consumed size: " + consumed.size());

                        for(int j = 0 ; j<5; j++ ) { //TODO deve essere un divisore del batch-size che essendo 25...
                            Set<String> keys = tbd.keySet();
                            if (keys.isEmpty())
                                throw new RuntimeException("Map should never be empty");

                            String key = keys.iterator().next();

                            //removes an element from the list
                            WriteRequest re = tbd.get(key).remove(0);
                            consumed.add(re);

                            if (tbd.get(key).isEmpty())
                                tbd.remove(key); //remopves eventually empty 'rows'
                        }

                        System.err.println("outgoing payload size: " + tbd.values().stream().collect(Collectors.summingInt(l -> l.size())));

                        return new BatchWriteItemResult().withUnprocessedItems(tbd);
                    }
                });

        ListenableFuture<WriteItemResult> f = null;
        for (int i = 0; i < 25; i++) {
            f = producer.addUserRecord(new AugmentedWriteRequest("YY", new WriteRequest()));
            assertEquals(0, producer.getSpillerCycles());
        }
        //wait for the future to be fulfilled
        f.get();

        //we caount Cycles as complete runs
        assertEquals(1, producer.getSpillerCycles());

        //but we actually did 25 calls
        assertEquals(25, consumed.size());


    }





}