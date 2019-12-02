package it.fabricalab.flink.dynamodb.sink;

import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.flink.annotation.VisibleForTesting;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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

    @VisibleForTesting
    public DynamoDBProducer(DynamoDBProducerConfiguration producerConfig, FlinkDynamoDBProducer.Client client, Runnable spiller) {
        //fire the spiller
        spillerExecutor.execute(spiller);
    }

    private int spillerCycles = 0;


    public DynamoDBProducer(DynamoDBProducerConfiguration producerConfig, FlinkDynamoDBProducer.Client client) {

        Runnable spiller = () -> {
            System.out.println(String.format("starting spiller task thread %s", Thread.currentThread().getName()));

            while (true) {
                try {
                    PayloadWithFuture payloadWithFuture = queue.take(); //blocking cause the queue is blocking
                    spillerCycles++;
                    System.err.println("consuming payload: " + payloadWithFuture.getPayload());

                    Map<String, List<WriteRequest>> payload = payloadWithFuture.getPayload();
                    int delayMillis = 5;
                    while (true) {
                        //TODO manca un try catch; per essere felici tutte le eccezioni thrown da aws sono unchecked
                        BatchWriteItemResult result = client.batchWriteItem(new BatchWriteItemRequest().withRequestItems(payload));
                        System.err.println("request sent");


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

                    System.err.println("request sent");
                    //TODO capire se serve ritornare il result e se serve in caso allora ritornare la lista di tutti
                    payloadWithFuture.getFuture().set(new WriteItemResult(true, null));

                    //here queuLatch should never been null
                    if (queue.isEmpty())
                        queueLatch.countDown();

                } catch (Throwable t) {
                    if (t instanceof InterruptedException && swallowInterruptedException)
                        continue;
                    t.printStackTrace();
                }
            }
        };

        //fire the spiller
        spillerExecutor.execute(spiller);
    }


    public int getOutstandingRecordsCount() {
        return currentNumberOfRequests;
    }

    public int getSpillerCycles() {
        return spillerCycles;
    }

    public ListenableFuture<WriteItemResult> addUserRecord(AugmentedWriteRequest value) {

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

    private void promoteUnderConstructionToQueue() {
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
            System.err.println("nothing to flush");
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
