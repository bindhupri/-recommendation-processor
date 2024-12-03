package com.sams.productrecommendations.transform;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.sams.productrecommendations.model.CosmosRyeRecommendationDTO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_RYE_CONTAINER;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_RYE_DATABASE;

@Slf4j
public class CosmosRyeIngestion extends DoFn<KV<Integer, CosmosRyeRecommendationDTO>, CosmosRyeRecommendationDTO> {


    private final String projectId;
    private  Integer batchSize;

    public CosmosRyeIngestion(String projectId, Integer batchSize){
        this.projectId = projectId;
        this.batchSize = batchSize;
    }

    private transient CosmosAsyncClient cosmosAsyncClient;

    @Setup
    public void buildCosmosAsyncClient() {
        cosmosAsyncClient = CosmosIngestionHelper.buildCosmosAsyncClient(projectId);
    }
    private static final Counter cosmosIngestionBatch = Metrics.counter(CosmosRyeIngestion.class, "cosmosIngestionBatch");

    private static final Duration MAX_BUFFER_DURATION = Duration.standardSeconds(1);
    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("count")
    private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();
    @StateId("buffer")
    private final StateSpec<BagState<KV<Integer, CosmosRyeRecommendationDTO>>> bufferedEvents = StateSpecs.bag();

    @TimerId("stale")
    private final TimerSpec staleSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window,
                               @StateId("buffer") BagState<KV<Integer, CosmosRyeRecommendationDTO>> bufferState,
                               @TimerId("stale") Timer staleTimer,
                               @StateId("count") ValueState<Integer> countState, @TimerId("expiry") Timer expiryTimer) {

        int count = 0;
        if (countState.read() != null) {
            count = countState.read();
        }
        if (count == 0) {
            staleTimer.offset(MAX_BUFFER_DURATION).setRelative();
        }
        count = count + 1;
        countState.write(count);
        bufferState.add(context.element());
        expiryTimer.set(window.maxTimestamp().minus(1));
        if (count >= batchSize) {
            List<CosmosRyeRecommendationDTO> productRecommendationsDTOList = new ArrayList<>();
            for (KV<Integer, CosmosRyeRecommendationDTO> queryBatch : bufferState.read()) {
                productRecommendationsDTOList.add(queryBatch.getValue());
            }
            context.output(ingestToCosmos(productRecommendationsDTOList));
            bufferState.clear();
            countState.clear();
            productRecommendationsDTOList.clear();
        }

    }

    @OnTimer("expiry")
    public void onExpiry(
            OnTimerContext context,
            @StateId("buffer") BagState<KV<Integer, CosmosRyeRecommendationDTO>> bufferState) {

        if (Boolean.FALSE.equals(bufferState.isEmpty().read())) {
            List<CosmosRyeRecommendationDTO> productRecommendationsDTOList = new ArrayList<>();
            for (KV<Integer, CosmosRyeRecommendationDTO> queryBatch : bufferState.read()) {
                productRecommendationsDTOList.add(queryBatch.getValue());
            }
            context.output(ingestToCosmos(productRecommendationsDTOList));
            bufferState.clear();
            productRecommendationsDTOList.clear();
        }
    }


    @OnTimer("stale")
    public void onStale(
            OnTimerContext context,
            @StateId("buffer") BagState<KV<Integer, CosmosRyeRecommendationDTO>> bufferState,
            @StateId("count") ValueState<Integer> countState) {
        if (Boolean.FALSE.equals(bufferState.isEmpty().read())) {
            List<CosmosRyeRecommendationDTO> productRecommendationsDTOList = new ArrayList<>();
            for (KV<Integer, CosmosRyeRecommendationDTO> queryBatch : bufferState.read()) {
                productRecommendationsDTOList.add(queryBatch.getValue());
            }
            context.output(ingestToCosmos(productRecommendationsDTOList));
            bufferState.clear();
            countState.clear();
            productRecommendationsDTOList.clear();
        }
    }

    // @FinishBundle
    public CosmosRyeRecommendationDTO ingestToCosmos(List<CosmosRyeRecommendationDTO> records){
        cosmosIngestionBatch.inc();
        Flux<CosmosRyeRecommendationDTO> recommendationList = Flux.fromIterable(records);
        CosmosAsyncDatabase cosmosDatabase = cosmosAsyncClient.getDatabase(COSMOS_RYE_DATABASE);
        CosmosAsyncContainer cosmosContainer = cosmosDatabase.getContainer(COSMOS_RYE_CONTAINER);
        Flux<CosmosItemOperation> cosmosItemOperations = recommendationList.map(
                ryeRecommendationDTO -> CosmosBulkOperations.getUpsertItemOperation(ryeRecommendationDTO, new PartitionKey(ryeRecommendationDTO.recommendationId())));
        return Objects.requireNonNull(cosmosContainer.executeBulkOperations(cosmosItemOperations).blockLast()).getResponse().getItem(CosmosRyeRecommendationDTO.class);

    }
    @Teardown
    public void closeCosmosConnection(){
        cosmosAsyncClient.close();
    }
}
