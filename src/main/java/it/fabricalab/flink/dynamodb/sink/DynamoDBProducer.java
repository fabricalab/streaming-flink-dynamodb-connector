package it.fabricalab.flink.dynamodb.sink;

import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class DynamoDBProducer {


    private static final int MAX_ELEMENTS_PER_REQUEST = 25; //Dynamodb MAX !!
    private static final int INITIAL_SPILLER_DELAY = 5; //

    private Map<String, List<WriteRequest>> currentlyUnderConstruction = new HashMap<>();
    private SettableFuture<WriteItemResult> currentFuture = SettableFuture.create();
    private int currentSize = 0;
    private int currentNumberOfRequests = 0;

    private CountDownLatch queueLatch = null;

    private LinkedBlockingQueue<PayloadWithFuture> queue = new LinkedBlockingQueue<>();

    @VisibleForTesting
    public Queue<PayloadWithFuture> getQueue() {
        return queue;
    }

    @VisibleForTesting
    public Map<String, List<WriteRequest>> getCurrentlyUnderConstruction() {
        return currentlyUnderConstruction;
    }

    @VisibleForTesting
    public ExecutorService getSpillerExecutor() {
        return spillerExecutor;
    }


    private ExecutorService spillerExecutor = Executors.newSingleThreadExecutor();


    private volatile boolean spillerDied = false;
    private volatile Throwable spillerTrowed = null;

    @VisibleForTesting
    public DynamoDBProducer(Properties producerProps, Runnable spiller, Consumer<Throwable> errorCallback) {
        //fire the spiller
        CompletableFuture.runAsync(spiller).whenComplete(new BiConsumer<Void, Throwable>() {
            @Override
            public void accept(Void aVoid, Throwable throwable) {
                spillerDied = true;
                spillerTrowed = throwable;
                errorCallback.accept(throwable);
            }
        });
    }

    private int spillerCycles = 0;


    private KeySelector<AugmentedWriteRequest, String> keySelector;
    private Set<String> seenKeys = new HashSet<>();

    public DynamoDBProducer(Properties producerProps,
                            FlinkDynamoDBProducer.Client client,
                            Consumer<Throwable> errorCallback) {
        this(producerProps, client, null, errorCallback);
    }

    public DynamoDBProducer(Properties producerProps,
                            FlinkDynamoDBProducer.Client client,
                            KeySelector<AugmentedWriteRequest, String> keySelector, Consumer<Throwable> errorCallback) {

        this.keySelector = keySelector;

        Runnable spiller = () -> {

            //the spiller is a infinite loop consuming the queue
            while (true) {
                try {
                    PayloadWithFuture payloadWithFuture = queue.take(); //blocking cause the queue is blocking
                    spillerCycles++;
                    //System.err.println("consuming payload: " + payloadWithFuture.getPayload());

                    Map<String, List<WriteRequest>> payload = payloadWithFuture.getPayload();
                    int delayMillis = INITIAL_SPILLER_DELAY;
                    while (true) {
                        BatchWriteItemResult result = null;
                        try {
                            log.debug("writing payload of keys-size {}: {}", payload.keySet().size(), payload);
                            result = client.batchWriteItem(new BatchWriteItemRequest().withRequestItems(payload));
                        } catch (ProvisionedThroughputExceededException e) { // this is recoverable

                            log.error("error writing to Dynamo: {}",e);
                            e.printStackTrace();
                            //so we treat it like the case with unprocessed items
                            result = new BatchWriteItemResult().withUnprocessedItems(payload);
                            //with an additional delay
                            Thread.sleep(delayMillis);

                        }
                        //System.err.println("request sent");


                        //a questo punto c'e' la questione noiosa che AWS potrebbe non aver 'consumato' tutti
                        //i record la scriviamo in un log per ora

                        if (result.getUnprocessedItems() == null || result.getUnprocessedItems().isEmpty()) {
                            //we decrement the list of the  items we consumed;
                            log.debug("all entries in payload consumed");

                            currentNumberOfRequests -= payload.values().stream()
                                    .collect(Collectors.summingInt(l -> l.size()));
                            break;
                        } else {

                            log.debug("UnprocessedItems left: {} ",result.getUnprocessedItems());

                            //we decrement the list of the  items we consumed;
                            Map<String, List<WriteRequest>> remainingPayload = result.getUnprocessedItems();
                            currentNumberOfRequests -= (
                                    payload.values().stream().collect(Collectors.summingInt(l -> l.size()))
                                            - remainingPayload.values().stream().collect(Collectors.summingInt(l -> l.size()))
                            );
                            payload = remainingPayload;
                            //exp delay as per doc
                            System.err.println("retry after delay of ms " + delayMillis);
                            Thread.sleep(delayMillis);
                            System.err.println("retrying");
                            delayMillis *= 2;

                            log.debug("retrying with payload {}", payload);

                        }

                    }

                    //System.err.println("request sent");
                    //TODO capire se serve ritornare il result e se serve in caso allora ritornare la lista di tutti
                    payloadWithFuture.getFuture().set(new WriteItemResult(true, null));

                    //here queueLatch should never been null
                    if (queue.isEmpty())
                        queueLatch.countDown();

                } catch (InterruptedException t) {
                    if (swallowInterruptedException)
                        continue;
                    log.error("InterruptedException {}", t);
                    t.printStackTrace();
                }
            }

        };


        CompletableFuture.runAsync(spiller).whenComplete(new BiConsumer<Object, Throwable>() {
            @Override
            public void accept(Object o, Throwable throwable) {
                spillerDied = true;
                spillerTrowed = throwable;
                errorCallback.accept(throwable);
            }
        });
    }


    public int getOutstandingRecordsCount() {
        return currentNumberOfRequests;
    }

    public int getSpillerCycles() {
        return spillerCycles;
    }

    public ListenableFuture<WriteItemResult> addUserRecord(AugmentedWriteRequest value) {

        propagateSpillerExceptions();

        if (keySelector != null) {
            try {
                String key = keySelector.getKey(value);
                if (seenKeys.contains(key)) {
                    //force flush
                    System.err.println("Force flush due to chunking, splitting at " + getCurrentlyUnderConstruction().size() + " out of " + MAX_ELEMENTS_PER_REQUEST);
                    promoteUnderConstructionToQueue(); //silently changes state
                    seenKeys.clear();
                }
                seenKeys.add(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        List<WriteRequest> l = currentlyUnderConstruction.get(value.getTableName());
        if (l == null) {
            l = new ArrayList<>();
            currentlyUnderConstruction.put(value.getTableName(), l);
        }
        l.add(value.getRequest());
        currentSize++;
        currentNumberOfRequests++;

        ListenableFuture<WriteItemResult> futureToReturn = currentFuture;

        if (currentSize >= MAX_ELEMENTS_PER_REQUEST) {
            promoteUnderConstructionToQueue(); //silently changes state
        }

        return futureToReturn;  //a single future for each request
    }


    private void propagateSpillerExceptions() {
        //control if we are still alive
        if (spillerDied) {
            if (spillerTrowed != null) {
                throw new RuntimeException("Spiller died with exception", spillerTrowed);
            }
            throw new RuntimeException("Spiller died without exception");
        }
    }

    private void promoteUnderConstructionToQueue() {
        propagateSpillerExceptions();
        //close the current map
        if (queueLatch == null || queueLatch.getCount() == 0)
            queueLatch = new CountDownLatch(1);
        queue.offer(new PayloadWithFuture(currentlyUnderConstruction, currentFuture));
        currentlyUnderConstruction = new HashMap<>();
        currentFuture = SettableFuture.create();
        currentSize = 0;
    }

    public void flush() {
        if (currentSize == 0) {
            //System.err.println("nothing to flush");
            return;
        }
        promoteUnderConstructionToQueue();
    }

    public void flushAndWait() throws InterruptedException {
        flush();
        queueLatch.await();
    }


    //it will cause an exception will be thrown, so we swallow it
    private volatile boolean swallowInterruptedException = false;

    public void destroy() {
        swallowInterruptedException = true;
        spillerExecutor.shutdownNow();
    }

}
