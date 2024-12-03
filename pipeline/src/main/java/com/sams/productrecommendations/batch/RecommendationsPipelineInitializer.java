package com.sams.productrecommendations.batch;

import com.sams.productrecommendations.config.BatchPipelineOptions;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;


import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.LANDING_COSMOS_FAILURE;
import static com.sams.productrecommendations.util.ApplicationConstants.MODULE_LANDING_PAGE;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_COSMOS_FAILURE;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_RECOMMENDATION;


@Slf4j
public class RecommendationsPipelineInitializer {

    public static void main(String[] args) {
        try {
            var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BatchPipelineOptions.class);
            var pipeline = Pipeline.create(options);
            switch (String.valueOf(options.getModule())) {
                case MODULE_LANDING_PAGE:
                    new LandingPageRecommendationProcessor().start(options, pipeline);
                    break;
                case RYE_RECOMMENDATION:
                    RyeRecommendationProcessor.processRecommendationsFromFile(options, pipeline);
                    break;
                case RYE_COSMOS_FAILURE:
                    RyeCosmosFailureProcessor.retryCosmosFailures(options, pipeline);
                    break;
                case LANDING_COSMOS_FAILURE:
                    CosmosFailureProcessor.retryCosmosFailures(options, pipeline);
                    break;
                default:
                    new GenericRecommendationProcessor().start(options, pipeline);
            }
        } catch (Exception e) {
            log.error("Error While Creating a Template:{}", e.getMessage());
        }
    }
}
