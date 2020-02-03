
package it.fabricalab.flink.dynamodb.sink;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import it.fabricalab.flink.dynamodb.sink.utils.AWSUtil;
import it.fabricalab.flink.dynamodb.sink.utils.TimeoutLatch;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The FlinkDynamodbProducer allows to produce from a Flink DataStream into Kinesis.
 */
@PublicEvolving
public class FlinkDynamoDBProducer extends RichSinkFunction<AugmentedWriteRequest> implements ListCheckpointed<AugmentedWriteRequest> {

    public static final String DYNAMODB_PRODUCER_TIMEOUT_PROPERTY = "flush-timeout";

    public static final String DYNAMODB_PRODUCER_METRIC_GROUP = "dynamodbProducer";

    public static final String DYNAMODB_PRODUCER_CHECKPOINTINGMODE_PROPERTY = "checkpointingMode";


    public static final String METRIC_BACKPRESSURE_CYCLES = "backpressureCycles";

    public static final String METRIC_OUTSTANDING_RECORDS_COUNT = "outstandingRecordsCount";

    private static final long serialVersionUID = 6447077318449477846L;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkDynamoDBProducer.class);

    /**
     * Properties to parametrize settings such as AWS service region, access key etc.
     */
    private final Properties configProps;
    private final int flushTimeout;
    private final CheckpointingMode checkpointingMode;

    /* Flag controlling the error behavior of the producer */
    private boolean failOnError = false;

    /* Maximum length of the internal record queue before backpressuring */
    private int queueLimit = Integer.MAX_VALUE;

    // --------------------------- Runtime fields ---------------------------

    /* Our Kinesis instance for each parallel Flink sink */
    private transient DynamoDBProducer producer;

    /* Backpressuring waits for this latch, triggered by record callback */
    private transient TimeoutLatch backpressureLatch;

    /* Callback handling failures */
    private transient FutureCallback<WriteItemResult> callback;

    /* Counts how often we have to wait for KPL because we are above the queue limit */
    private transient Counter backpressureCycles;

    /* Field for async exception */
    private transient volatile Throwable thrownException;


    /* scheduler service for timeout flushes */
    private ScheduledExecutorService scheduler = null;

    private KeySelector<AugmentedWriteRequest, String> keySelector = null;
    // --------------------------- Initialization and configuration  ---------------------------


    /**
     * Create a new FlinkDynamodbProducer.
     *
     * @param configProps The properties used to configure DynamodbProducer, including AWS credentials and AWS region
     */
    public FlinkDynamoDBProducer(Properties configProps) {
        this(configProps, null);
    }

    /**
     * Create a new FlinkDynamodbProducer.
     *
     * @param configProps The properties used to configure DynamodbProducer, including AWS credentials and AWS region
     * @param keySelector Key Selector to avoid primary-key conflicts in batchWriteItem
     */
    public FlinkDynamoDBProducer(Properties configProps, KeySelector<AugmentedWriteRequest, String> keySelector) {
        checkNotNull(configProps, "configProps can not be null");
        this.configProps = configProps;
        this.keySelector = keySelector;

        //per default no flush
        this.flushTimeout = Integer.parseInt(configProps.getProperty(DYNAMODB_PRODUCER_TIMEOUT_PROPERTY, "-1"));


        //per default flushBefore
        CheckpointingMode checkpointingMode = CheckpointingMode.FlushBefore;
        String s = configProps.getProperty(DYNAMODB_PRODUCER_CHECKPOINTINGMODE_PROPERTY);

        if (s != null)
            if (s.equals(CheckpointingMode.ListState.name()))
                checkpointingMode = CheckpointingMode.ListState;

        this.checkpointingMode = checkpointingMode;
        LOG.info("Flink DynamoDB producer checkpointingMode {}, flushTimeout {}", this.checkpointingMode, this.flushTimeout);

    }

    public enum CheckpointingMode {
        FlushBefore, //the producer relays checkpoints and flushes. no state needed
        ListState, //the produces persists flight requests in list state
    }

    /**
     * If set to true, the producer will immediately fail with an exception on any error.
     * Otherwise, the errors are logged and the producer goes on.
     *
     * @param failOnError Error behavior flag
     */
    public void setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
    }

    /**
     * The {@link DynamoDBProducer} holds an unbounded queue internally. To avoid memory
     * problems under high loads, a limit can be employed above which the internal queue
     * will be flushed, thereby applying backpressure.
     *
     * @param queueLimit The maximum length of the internal queue before backpressuring
     */
    public void setQueueLimit(int queueLimit) {
        checkArgument(queueLimit > 0, "queueLimit must be a positive number");
        this.queueLimit = queueLimit;
    }


    //what we actually need from DynamoDB client
    public interface Client {
        BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequestx);
    }
    // --------------------------- Lifecycle methods ---------------------------

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // check and pass the configuration properties
        //for future use
        //DynamoDBProducerConfiguration producerConfig = DynamoDBProducerUtils.getValidatedProducerConfiguration(configProps);

        Client client = getClient(configProps);
        producer = getProducer(configProps, client, keySelector);

        final MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(DYNAMODB_PRODUCER_METRIC_GROUP);
        this.backpressureCycles = metricGroup.counter(METRIC_BACKPRESSURE_CYCLES);
        metricGroup.gauge(METRIC_OUTSTANDING_RECORDS_COUNT, producer::getOutstandingRecordsCount);

        backpressureLatch = new TimeoutLatch();
        callback = new FutureCallback<WriteItemResult>() {
            @Override
            public void onSuccess(WriteItemResult result) {
                backpressureLatch.trigger();
                if (!result.isSuccessful()) {  //la nodtra logica e' piu' complessa visto che ci sono i retry ecc
                    if (failOnError) {
                        // only remember the first thrown exception
                        if (thrownException == null) {
                            thrownException = new RuntimeException("Record was not sent successful");
                        }
                    } else {
                        LOG.warn("Record was not sent successful");
                    }
                }
            }

            @Override
            public void onFailure(Throwable t) {
                backpressureLatch.trigger();
                if (failOnError) {
                    thrownException = t;
                } else {
                    LOG.warn("An exception occurred while processing a record", t);
                }
            }
        };


        //eventually prepare the scheduler
        if (flushTimeout > 0) {
            scheduler = Executors.newScheduledThreadPool(1);
            Runnable timerRunnable = () -> {
                LOG.debug("Flushing DynamoDBProducer queue, timeout: {}", flushTimeout);
                producer.flush();
            };
            scheduler.scheduleAtFixedRate(timerRunnable, flushTimeout, flushTimeout, TimeUnit.MILLISECONDS);
        }

        LOG.info("Started DYNAMODB producer instance for region '{}'", configProps.get("aws.region"));

        if (initialQueue != null) {

            LOG.info("restoring {} elements from checkpointed state", initialQueue.size());

            //TODO that's damn identical to 'invoke' last part
            for (AugmentedWriteRequest value : initialQueue) {
                ListenableFuture<WriteItemResult> cb = producer.addUserRecord(value);
                Futures.addCallback(cb, callback, ForkJoinPool.commonPool());
            }
        }
    }


    @Override
    public void invoke(AugmentedWriteRequest value, Context context) throws Exception {
        if (this.producer == null) {
            throw new RuntimeException("DynamoDB producer has been closed");
        }

        checkAndPropagateAsyncError();
        boolean didWaitForFlush = enforceQueueLimit();


        if (didWaitForFlush) {
            checkAndPropagateAsyncError();
        }

        //eventually schedule flushes


        ListenableFuture<WriteItemResult> cb = producer.addUserRecord(value);
        Futures.addCallback(cb, callback, ForkJoinPool.commonPool()); //da capire
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing producer");
        super.close();

        if (producer != null) {
            LOG.info("Flushing outstanding {} records", producer.getOutstandingRecordsCount());
            // try to flush all outstanding records
            flushSync();

            LOG.info("Flushing done. Destroying producer instance.");
            producer.destroy();
            producer = null;
        }

        // make sure we propagate pending errors
        checkAndPropagateAsyncError();
    }


    @Override
    public List<AugmentedWriteRequest> snapshotState(long checkpointId, long timestamp) throws Exception {


        if(checkpointingMode == CheckpointingMode.ListState) {

            // check for asynchronous errors and fail the checkpoint if necessary
            checkAndPropagateAsyncError();


            //rebuild AugmentedRequests from underConstruction map
            Stream<AugmentedWriteRequest> underConstruction =
                    producer.getCurrentlyUnderConstruction().entrySet().stream()
                            .flatMap(e -> e.getValue().stream().map( v -> new AugmentedWriteRequest(e.getKey(), v)));

            //queue is essentially a set of underConstruction maps
            Stream<AugmentedWriteRequest> inQueue = producer.getQueue().stream().map(q -> q.getPayload().entrySet().stream() )
                    .flatMap(Function.identity()) //flatten
                    .flatMap(e -> e.getValue().stream().map( v -> new AugmentedWriteRequest(e.getKey(), v)));


            return Stream.concat(inQueue, underConstruction).collect(Collectors.toList());

        }

        // check for asynchronous errors and fail the checkpoint if necessary
        checkAndPropagateAsyncError();

        flushSync();
        if (producer.getOutstandingRecordsCount() > 0) {
            throw new IllegalStateException(
                    "Number of outstanding records must be zero at this point: " + producer.getOutstandingRecordsCount());
        }

        // if the flushed requests has errors, we should propagate it also and fail the checkpoint
        checkAndPropagateAsyncError();

        return Collections.emptyList();

    }


    /*** from ListSchecpointed documentaion */
    /**
     * <p><b>Important:</b> When implementing this interface together with {RichFunction},
     * then the {@code restoreState()} method is called before {RichFunction#open(Configuration)}.
     */
    private List<AugmentedWriteRequest> initialQueue = null;

    @Override
    public void restoreState(List<AugmentedWriteRequest> state) throws Exception {
        initialQueue = new ArrayList<>(state); //should we make a deep copy ?
        //we will inject the initial elements just after initialization in 'open'
    }


    // --------------------------- Utilities ---------------------------

    /**
     * Creates a {@link DynamoDBProducer}.
     * Exposed so that tests can inject mock producers easily.
     */
    @VisibleForTesting
    protected DynamoDBProducer getProducer(Properties producerProps, Client client, KeySelector<AugmentedWriteRequest, String> keySelector) {
        return new DynamoDBProducer(producerProps, client, keySelector, throwable -> {
            LOG.error("An exception occurred in the producer", throwable);
            thrownException = throwable;
        });
    }

    /**
     * Creates a {@link Client}.
     * Exposed so that tests can inject mock producers easily.
     *
     * @param producerProps
     */
    @VisibleForTesting
    protected Client getClient(Properties producerProps) {
        AmazonDynamoDB actualClient = AWSUtil.createDynamoDBClient(configProps);
        return (r) -> actualClient.batchWriteItem(r);
    }


    /**
     * Check if there are any asynchronous exceptions. If so, rethrow the exception.
     */
    private void checkAndPropagateAsyncError() throws Exception {
        if (thrownException != null) {
            String errorMessages = "_EMPTY_";
//			if (thrownException instanceof UserRecordFailedException) {
//				List<Attempt> attempts = ((UserRecordFailedException) thrownException).getResult().getAttempts();
//				for (Attempt attempt: attempts) {
//					if (attempt.getErrorMessage() != null) {
//						errorMessages += attempt.getErrorMessage() + "\n";
//					}
//				}
//			}
            if (failOnError) {
                throw new RuntimeException("An exception was thrown while processing a record: " + errorMessages, thrownException);
            } else {
                LOG.warn("An exception was thrown while processing a record: {}", thrownException, errorMessages);

                // reset, prevent double throwing
                thrownException = null;
            }
        }
    }

    /**
     * If the internal queue of the {@link DynamoDBProducer} gets too long,
     * flush some of the records until we are below the limit again.
     * We don't want to flush _all_ records at this point since that would
     * break record aggregation.
     *
     * @return boolean whether flushing occurred or not
     */
    private boolean enforceQueueLimit() {
        int attempt = 0;
        while (producer.getOutstandingRecordsCount() >= queueLimit) {
            backpressureCycles.inc();
            if (attempt >= 10) {
                LOG.warn("Waiting for the queue length to drop below the limit takes unusually long, still not done after {} attempts.", attempt);
            }
            attempt++;
            try {
                backpressureLatch.await(100);
            } catch (InterruptedException e) {
                LOG.warn("Flushing was interrupted.");
                break;
            }
        }
        return attempt > 0;
    }

    /**
     * This implementation releases the block on flushing if an interruption occurred.
     */
    private void flushSync() throws Exception {
        while (producer.getOutstandingRecordsCount() > 0) {
            try {
                producer.flushAndWait();
            } catch (InterruptedException e) {
                LOG.warn("Flushing was interrupted.");
                break;
            }
        }
    }
}
