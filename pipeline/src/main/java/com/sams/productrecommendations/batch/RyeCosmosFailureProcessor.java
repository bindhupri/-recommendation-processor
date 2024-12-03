package com.sams.productrecommendations.batch;

import com.sams.productrecommendations.config.BatchPipelineOptions;
import com.sams.productrecommendations.model.CosmosRyeRecommendationDTO;
import com.sams.productrecommendations.transform.AuditTransformer;
import com.sams.productrecommendations.transform.CosmosIngestionHelper;
import com.sams.productrecommendations.transform.RecommendationTransformer;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_COSMOS_FAILURE_READ;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_FAILURE_READ_COUNT;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_COSMOS_FAILURE_MODEL_TRANSFORM;


@Slf4j
public class RyeCosmosFailureProcessor {
    private RyeCosmosFailureProcessor() {
    }

    public static void retryCosmosFailures(BatchPipelineOptions options, Pipeline pipeline) {
        PCollection<CosmosRyeRecommendationDTO> cosmosCollection = pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(AUDIT_COSMOS_FAILURE_READ, new AuditTransformer.AuditRyeCosmosFailures(COSMOS_FAILURE_READ_COUNT, options.getProject()))
                .apply(RYE_COSMOS_FAILURE_MODEL_TRANSFORM, ParDo.of(new RecommendationTransformer.RyeJsonToCosmosModel()));
        CosmosIngestionHelper.writeRyeToCosmos(options, cosmosCollection);
        pipeline.run().waitUntilFinish();
    }
}
