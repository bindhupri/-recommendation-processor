package com.sams.productrecommendations.io;

import com.google.auto.value.AutoValue;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.sams.productrecommendations.util.SecretManagerUtil;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Instant;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.*;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

/*
 An IO to write to Apache Cosmos
*/
@Slf4j
@SuppressWarnings({"java:S2696","java:S112"})
public class CosmosIO {
    private static CosmosClient client;

    private CosmosIO() {
    }

    public static <T> Write<T> write(String projectId, String partitionKey, Class<T> classType) {
        return new AutoValue_CosmosIO_Write.Builder<T>()
                .setClassType(classType)
                .setBatchSize(10)
                .setPartitionKey(partitionKey)
                .setHost(SecretManagerUtil.getSecret(projectId, HOST_SECRET_NAME))
                .setKey(SecretManagerUtil.getSecret(projectId, ACCOUNY_KEY_SECRET_NAME))
                .build();
    }

    /**
     * A {@link PTransform} to wire into Cosmos. See {@link CosmosIO} for details on usage and
     * configuration.
     */
    @AutoValue
    public abstract static class Write<T> extends PTransform<PCollection<T>, PCollection<T>> {

        @Pure
        abstract @Nullable
        Class<T> classType();

        @Pure
        abstract @Nullable
        Coder<T> coder();

        @Pure
        abstract @Nullable
        String database();

        @Pure
        abstract @Nullable
        String container();

        @Pure
        abstract @Nullable
        String host();

        @Pure
        abstract @Nullable
        String key();

        @Pure
        abstract String partitionKey();

        @Pure
        abstract long batchSize();

        @Pure
        abstract Builder<T> builder();

        @AutoValue.Builder
        abstract static class Builder<T> {

            abstract Builder<T> setClassType(Class<T> classType);

            abstract Builder<T> setDatabase(String database);

            abstract Builder<T> setContainer(String container);

            abstract Builder<T> setHost(String host);

            abstract Builder<T> setKey(String key);

            abstract Builder<T> setPartitionKey(String partitionKey);

            abstract Builder<T> setBatchSize(long batchSize);

            abstract Builder<T> setCoder(Coder<T> coder);

            abstract Write<T> build();
        }

        /**
         * Specify the Cosmos database to read from.
         */
        public Write<T> withDatabase(String database) {
            checkArgument(database != null, "database can not be null");
            checkArgument(!database.isEmpty(), "database can not be empty");
            return builder().setDatabase(database).build();
        }

        public Write<T> withContainer(String container) {
            checkArgument(container != null, "container can not be null");
            checkArgument(!container.isEmpty(), "container can not be empty");
            return builder().setContainer(container).build();
        }

        public Write<T> withBatchSize(long batchSize) {
            return builder().setBatchSize(batchSize).build();
        }

        public Write<T> withPartitionKey(String partitionKey) {
            return builder().setPartitionKey(partitionKey).build();
        }

        @Override
        public void validate(PipelineOptions pipelineOptions) {
            checkState(
                    database() != null,
                    "CosmosIO requires a database to be set via " + "withDatabase(database)");
            checkState(
                    container() != null,
                    "CosmosIO requires a valid container to be set via withContainer(container)");

            checkState(
                    classType() != null,
                    "CosmosIO requires an classType to be set via withClassType(classType)");
        }

        @Override
        public PCollection<T> expand(PCollection<T> input) {
            PCollection<List<T>> iterablePCollection = batchElements(input, batchSize());
            return iterablePCollection.apply(ParDo.of(new WriteFn<>(this)));
        }
    }

    static <T> PCollection<List<T>> batchElements(PCollection<T> input, long batchSize) {
        return input.apply(
                ParDo.of(
                        new DoFn<T, List<T>>() {
                            transient List<T> outputList;

                            @ProcessElement
                            public void process(ProcessContext c) {
                                if (outputList == null) {
                                    outputList = new ArrayList<>();
                                }
                                outputList.add(c.element());
                                if (outputList.size() > batchSize) {
                                    c.output(outputList);
                                    outputList = null;
                                }
                            }

                            @FinishBundle
                            public void finish(FinishBundleContext c) {
                                if (outputList != null && !outputList.isEmpty()) {
                                    c.output(outputList, Instant.now(), GlobalWindow.INSTANCE);
                                }
                                outputList = null;
                            }
                        }));
    }

    static class WriteFn<T> extends DoFn<List<T>, T> {
        private final Write<T> spec;

        WriteFn(Write<T> spec) {
            this.spec = spec;
        }

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            flush(ctx, ctx.element());
        }

        private void flush(ProcessContext context, List<T> records) {
            if (client == null) {
                var retryOptions = new ThrottlingRetryOptions();
                retryOptions.setMaxRetryAttemptsOnThrottledRequests(MAX_RETRY_ATTEMPTS_ON_THROTTLED_REQUESTS);
                retryOptions.setMaxRetryWaitTime(Duration.ofSeconds(MAX_RETRY_WAIT_TIME));
                var directConnectionConfig = DirectConnectionConfig.getDefaultConfig();
                directConnectionConfig.setConnectTimeout(Duration.ofMinutes(DIRECT_CONFIG_CONNECT_TIMEOUT));
                client =
                        new CosmosClientBuilder()
                                .endpoint(spec.host())
                                .key(spec.key())
                                .throttlingRetryOptions(retryOptions)
                                .preferredRegions(Collections.emptyList())
                                .consistencyLevel(ConsistencyLevel.CONSISTENT_PREFIX)
                                .multipleWriteRegionsEnabled(Boolean.FALSE)
                                .clientTelemetryEnabled(Boolean.FALSE)
                                .contentResponseOnWriteEnabled(true)
                                .userAgentSuffix("")
                                .directMode(directConnectionConfig)
                                .buildClient();
            }
            CosmosClient cosmosClient = Preconditions.checkArgumentNotNull(client);
            try {
                CosmosDatabase cosmosDatabase = cosmosClient.getDatabase(spec.database());
                CosmosContainer cosmosContainer = cosmosDatabase.getContainer(spec.container());
                List<CosmosItemOperation> cosmosItemOperations =
                        records.stream().map(this::getCosmosItemOperation).toList();
                cosmosContainer
                        .executeBulkOperations(cosmosItemOperations)
                        .forEach(
                                objectCosmosBulkOperationResponse ->
                                        this.handleBulkOperationsResponse(context, objectCosmosBulkOperationResponse));
            } catch (Exception e) {
                log.error("Error occurred while executing flush operation.", e);
                records.forEach(context::output);
            }
        }

        /* The maximum number of elements that will be included in a batch. */

        private void handleBulkOperationsResponse(
                ProcessContext context,
                CosmosBulkOperationResponse<Object> objectCosmosBulkOperationResponse) {
            CosmosBulkItemResponse cosmosBulkItemResponse =
                    objectCosmosBulkOperationResponse.getResponse();
            if (objectCosmosBulkOperationResponse.getException() != null) {
                context.output(objectCosmosBulkOperationResponse.getOperation().getItem());
                log.error(
                        "Error while executing upsert:", objectCosmosBulkOperationResponse.getException());
            } else if (cosmosBulkItemResponse == null || !cosmosBulkItemResponse.isSuccessStatusCode()) {
                context.output(objectCosmosBulkOperationResponse.getOperation().getItem());
                log.error(
                        "Upsert operation for item did not complete, {} response code.",
                        cosmosBulkItemResponse != null ? cosmosBulkItemResponse.getStatusCode() : "N/A");
            } else {
                if (log.isDebugEnabled())
                    log.debug("Item ingested to cosmos");
            }
        }

        private CosmosItemOperation getCosmosItemOperation(T t) {
            try {
                Field field = t.getClass().getDeclaredField(spec.partitionKey());
                return CosmosBulkOperations.getUpsertItemOperation(t, new PartitionKey(field.get(t)));
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}