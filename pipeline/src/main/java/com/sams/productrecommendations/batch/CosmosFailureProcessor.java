package com.sams.productrecommendations.batch;

import com.sams.productrecommendations.config.BatchPipelineOptions;
import com.sams.productrecommendations.model.CosmosProductRecommendationDTO;
import com.sams.productrecommendations.transform.CosmosIngestionHelper;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.CATEGORY_ID;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_DOCUMENT_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.ID;
import static com.sams.productrecommendations.util.ApplicationConstants.MODEL_TYPE_NAME;
import static com.sams.productrecommendations.util.ApplicationConstants.PROCESS_RECO_COUNTER;
import static com.sams.productrecommendations.util.ApplicationConstants.READ_COSMOS_INGESTION_FAILURE_FILES;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDATION_ID;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDATION_ID_TYPE;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDED_ITEMS;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDED_PRODUCTS;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDED_UPCS;
import static com.sams.productrecommendations.util.ApplicationConstants.STRATEGY_NAME;
import static com.sams.productrecommendations.util.ApplicationConstants.TRANSFORM_FAILURE_PRODUCT_MODEL;
import static com.sams.productrecommendations.util.ApplicationConstants.TTL;

@Slf4j
public class CosmosFailureProcessor {
    private CosmosFailureProcessor() {
    }

    public static void retryCosmosFailures(BatchPipelineOptions options, Pipeline pipeline) {
        try {
            PCollection<CosmosProductRecommendationDTO> failureRecommendations = pipeline.apply(READ_COSMOS_INGESTION_FAILURE_FILES, ParquetIO.read(new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(COSMOS_DOCUMENT_PARQUET_SCHEMA))).from(options.getInputFile()))
                    .apply(TRANSFORM_FAILURE_PRODUCT_MODEL, ParDo.of(new DoFn<GenericRecord, CosmosProductRecommendationDTO>() {
                        private final Counter processFailureRecoCounter = Metrics.counter(CosmosFailureProcessor.class, PROCESS_RECO_COUNTER);

                        @ProcessElement
                        public void processElement(@Element GenericRecord failureRecommendation, OutputReceiver<CosmosProductRecommendationDTO> outputReceiver) {
                            try {
                                outputReceiver.output(new CosmosProductRecommendationDTO(String.valueOf(failureRecommendation.get(ID)),
                                        String.valueOf(failureRecommendation.get(RECOMMENDATION_ID)),
                                        String.valueOf(failureRecommendation.get(RECOMMENDATION_ID_TYPE)),
                                        String.valueOf(failureRecommendation.get(STRATEGY_NAME)),
                                        String.valueOf(failureRecommendation.get(MODEL_TYPE_NAME)),
                                        requireNonNullList(String.valueOf(failureRecommendation.get(RECOMMENDED_PRODUCTS))),
                                        requireNonNullList(String.valueOf(failureRecommendation.get(RECOMMENDED_UPCS))),
                                        requireNonNullList(String.valueOf(failureRecommendation.get(RECOMMENDED_ITEMS))),
                                        defaultCheckCategory(String.valueOf(failureRecommendation.get(CATEGORY_ID))),
                                        defaultCheckInteger(String.valueOf(failureRecommendation.get(TTL)))));
                                processFailureRecoCounter.inc();
                            } catch (Exception e) {
                                log.error(e.getMessage());
                            }
                        }

                        List<String> requireNonNullList(String input) {
                            return input != null && !input.equalsIgnoreCase("null") ? List.of(input.split(",")) : List.of();
                        }

                        Integer defaultCheckInteger(String input) {
                            return input != null && !input.equalsIgnoreCase("null") ? Integer.parseInt(input) : 1;
                        }

                        Long defaultCheckCategory(String categoryId) {
                            return categoryId != null && !categoryId.equalsIgnoreCase("null") ? Long.parseLong(categoryId) : 0;
                        }
                    }));
            CosmosIngestionHelper.writeToCosmos(failureRecommendations, options);
            pipeline.run().waitUntilFinish();
        } catch (Exception e) {
            log.error("Error While Building Pipeline:{}", e.getMessage());
        }
    }
}
