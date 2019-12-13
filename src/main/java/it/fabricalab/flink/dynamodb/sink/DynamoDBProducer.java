package it.fabricalab.flink.dynamodb.sink;

import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.flink.annotation.VisibleForTesting;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public class DynamoDBProducer {


    private static final int MAX_ELEMENTS_PER_REQUEST = 25; //Dynamodb MAX !!
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
    private volatile Throwable spillerThrowed = null;

    @VisibleForTesting
    public DynamoDBProducer(Properties producerProps,Runnable spiller, Consumer<Throwable> errorCallback) {
        //fire the spiller
        CompletableFuture.runAsync(spiller).whenComplete(new BiConsumer<Void, Throwable>() {
            @Override
            public void accept(Void aVoid, Throwable throwable) {
                spillerDied = true;
                spillerThrowed = throwable;
                errorCallback.accept(throwable);
            }
        });
    }

    private int spillerCycles = 0;


    public DynamoDBProducer(Properties producerProps, FlinkDynamoDBProducer.Client client, Consumer<Throwable> errorCallback) {

        Runnable spiller = () -> {
            //System.out.println(String.format("starting spiller task thread %s", Thread.currentThread().getName()));

            while (true) {
                try {
                    PayloadWithFuture payloadWithFuture = queue.take(); //blocking cause the queue is blocking
                    spillerCycles++;
                    //System.err.println("consuming payload: " + payloadWithFuture.getPayload());

                    Map<String, List<WriteRequest>> payload = payloadWithFuture.getPayload();
                    int delayMillis = 5;
                    while (true) {
                        BatchWriteItemResult result = null;
                        try {
                           result = client.batchWriteItem(new BatchWriteItemRequest().withRequestItems(payload));
                        } catch (ProvisionedThroughputExceededException e) { // this is recoverable

                            e.printStackTrace();
                            //so we trat it like the case with unprocessed items
                            result = new BatchWriteItemResult().withUnprocessedItems(payload);
                            //with an additional delay
                            Thread.sleep(delayMillis);

                        }
                        //System.err.println("request sent");


                        //a questo punto c'e' la questione noiosa che AWS potrebbe non aver 'consumato' tutti
                        //i record la scriviamo in un log per ora

                        if (result.getUnprocessedItems() == null || result.getUnprocessedItems().isEmpty()) {
                            //we decrement the list of the  items we consumed;
                            currentNumberOfRequests -= payload.values().stream()
                                    .collect(Collectors.summingInt(l -> l.size()));
                            break;
                        } else {
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
                    t.printStackTrace();
                }
            }

        };


        CompletableFuture.runAsync(spiller).whenComplete(new BiConsumer<Object, Throwable>() {
            @Override
            public void accept(Object o, Throwable throwable) {
                spillerDied = true;
                spillerThrowed = throwable;
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
            promoteUnderConstructionToQueue(); //silentrly changes state
        }

        return futureToReturn;  //a single future for each request
    }


    private void propagateSpillerExceptions() {
        //control if we are still alive
        if(spillerDied ) {
            if(spillerThrowed != null) {
                throw new RuntimeException("Spiller died with exception", spillerThrowed);
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
