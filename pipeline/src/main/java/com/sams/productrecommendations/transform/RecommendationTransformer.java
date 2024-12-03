package com.sams.productrecommendations.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sams.productrecommendations.config.BatchPipelineOptions;
import com.sams.productrecommendations.model.CosmosRyeRecommendationDTO;
import com.sams.productrecommendations.model.LandingColdMembershipRecoModel;
import com.sams.productrecommendations.model.MembershipCardHolderDTO;
import com.sams.productrecommendations.model.CardHolderRecommendationDTO;
import com.sams.productrecommendations.model.MembershipRecommendationDTO;
import com.sams.productrecommendations.model.RyeRecommendationDTO;
import com.sams.productrecommendations.util.ObjectMapperHelper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.CATEGORY;
import static com.sams.productrecommendations.util.ApplicationConstants.GENERIC_AVRO_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.GENERIC_RECOMMENDATION_COUNTER;
import static com.sams.productrecommendations.util.ApplicationConstants.LANDING_RECOMMENDATION_COUNTER;
import static com.sams.productrecommendations.util.ApplicationConstants.LAST_PURCHASE_DATE;
import static com.sams.productrecommendations.util.ApplicationConstants.LAST_PURCHASE_UNITS;
import static com.sams.productrecommendations.util.ApplicationConstants.MEMBERSHIPUUID_AVRO_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.MEMBERSHIP_NUMBER;
import static com.sams.productrecommendations.util.ApplicationConstants.MEMBERSHIP_UUID;
import static com.sams.productrecommendations.util.ApplicationConstants.MEMBERSHIP_UUID_COUNTER;

import static com.sams.productrecommendations.util.ApplicationConstants.RYE_COSMOS_FAILURE_COUNTER;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_COSMOS_RETRY_COUNTER;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_PRODUCT_ID;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_RECOMMENDATION_COUNTER;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_COLD_AVRO_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_RECOMMENDATION_COUNTER;
import static com.sams.productrecommendations.util.ApplicationConstants.SOURCE_ID;
import static com.sams.productrecommendations.util.ApplicationConstants.MODEL_TYPE;
import static com.sams.productrecommendations.util.ApplicationConstants.MODULE_LANDING_PAGE;

import static com.sams.productrecommendations.util.ApplicationConstants.RANKED_PRODUCTS;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_RECOMMENDATION;

@Slf4j
public class RecommendationTransformer {
    private RecommendationTransformer() {
    }

    public static class ProductRecommendationModel extends DoFn<GenericRecord, MembershipRecommendationDTO> {

        private final String module;

        public ProductRecommendationModel(String module) {
            this.module = module;
        }

        private final Counter savingsRecoCounter = Metrics.counter(ProductRecommendationModel.class, SAVINGS_RECOMMENDATION_COUNTER);
        private final Counter landingPageRecoCounter = Metrics.counter(ProductRecommendationModel.class, LANDING_RECOMMENDATION_COUNTER);
        private final Counter genericRecoCounter = Metrics.counter(ProductRecommendationModel.class, GENERIC_RECOMMENDATION_COUNTER);

        @ProcessElement
        public void processRecord(ProcessContext c) {
            GenericRecord genericRecord = c.element();
            updateCounters(module);
            try {
                c.output(new MembershipRecommendationDTO(String.valueOf(genericRecord.get(SOURCE_ID).toString()), String.valueOf(genericRecord.get(RANKED_PRODUCTS)), String.valueOf(genericRecord.get(MODEL_TYPE))));
            } catch (Exception e) {
                log.error("ErrorWhile Processing Record:{}", genericRecord.toString());
                log.error("Exception:{}", e.getMessage());
            }
        }

        void updateCounters(String module) {
            switch (module.toLowerCase()) {
                case SAVINGS_RECOMMENDATION -> savingsRecoCounter.inc();
                case MODULE_LANDING_PAGE -> landingPageRecoCounter.inc();
                default -> genericRecoCounter.inc();
            }
        }
    }

    public static class LandingColdProductRecommendationTransform extends DoFn<GenericRecord, LandingColdMembershipRecoModel> {
        private final Counter landingPageRecoCounter = Metrics.counter(ProductRecommendationModel.class, LANDING_RECOMMENDATION_COUNTER);

        @ProcessElement
        public void processRecord(ProcessContext c) {
            GenericRecord genericRecord = c.element();
            landingPageRecoCounter.inc();
            try {
                c.output(new LandingColdMembershipRecoModel(String.valueOf(genericRecord.get(SOURCE_ID).toString()), String.valueOf(genericRecord.get(RANKED_PRODUCTS)), String.valueOf(genericRecord.get(MODEL_TYPE)), String.valueOf(genericRecord.get(CATEGORY))));
            } catch (Exception e) {
                log.error("LandingColdProductRecommendationModel : ErrorWhile Processing Record:{}", genericRecord.toString());
                log.error("LandingColdProductRecommendationModel : Exception:{}", e.getMessage());
            }
        }

    }

    public static class MembershipUUIDToGenericRecord extends DoFn<CardHolderRecommendationDTO, GenericRecord> {

        @ProcessElement
        public void processElement(@Element CardHolderRecommendationDTO membership, OutputReceiver<GenericRecord> out) {
            GenericRecord genericRecords = mapToGenericRecord(membership);
            out.output(genericRecords);
        }

        public GenericRecord mapToGenericRecord(CardHolderRecommendationDTO uuidRecommendation) {
            GenericRecord genericRecord =
                    new GenericData.Record(new Schema.Parser().parse(MEMBERSHIPUUID_AVRO_SCHEMA));
            genericRecord.put(MEMBERSHIP_UUID, uuidRecommendation.membershipUUID());
            genericRecord.put(RANKED_PRODUCTS, uuidRecommendation.rankedProducts());
            genericRecord.put(MODEL_TYPE, uuidRecommendation.modelType());
            return genericRecord;
        }
    }

    public static class RecommendationDtoToGenericRecord extends DoFn<MembershipRecommendationDTO, GenericRecord> {

        @ProcessElement
        public void processElement(ProcessContext pc, @Element MembershipRecommendationDTO membershipRecommendationDTO, OutputReceiver<GenericRecord> out) {
            String module = pc.getPipelineOptions().as(BatchPipelineOptions.class).getModule().toString();
            GenericRecord genericRecord = new GenericData.Record(getAvroSchema(module));
            genericRecord.put(SOURCE_ID, membershipRecommendationDTO.id());
            genericRecord.put(RANKED_PRODUCTS, membershipRecommendationDTO.recommendations());
            genericRecord.put(MODEL_TYPE, membershipRecommendationDTO.modelType());
            out.output(genericRecord);
        }
    }

    public static Schema getAvroSchema(String module) {
        if (module.equalsIgnoreCase(SAVINGS_RECOMMENDATION)) {
            return new Schema.Parser().parse(SAVINGS_COLD_AVRO_SCHEMA);
        } else {
            return new Schema.Parser().parse(GENERIC_AVRO_SCHEMA);
        }
    }

    public static class BigQueryMembershipModelToUUIDModel extends DoFn<MembershipCardHolderDTO, CardHolderRecommendationDTO> {
        private final Counter membershipUUIDCounter = Metrics.counter(BigQueryMembershipModelToUUIDModel.class, MEMBERSHIP_UUID_COUNTER);

        @ProcessElement
        public void processElement(@Element MembershipCardHolderDTO bigqueryResponse, OutputReceiver<CardHolderRecommendationDTO> outputReceiver) {
            membershipUUIDCounter.inc();
            outputReceiver.output(new CardHolderRecommendationDTO(bigqueryResponse.membershipUUID(), bigqueryResponse.rankedProducts(), bigqueryResponse.modelType()));
        }
    }

    public static class RyeRecommendationModel extends DoFn<GenericRecord, RyeRecommendationDTO> {
        private final Counter ryeRecoCounter = Metrics.counter(RyeRecommendationModel.class, RYE_RECOMMENDATION_COUNTER);

        @ProcessElement
        public void processElement(@Element GenericRecord ryeRecord, OutputReceiver<RyeRecommendationDTO> outputReceiver) {
            ryeRecoCounter.inc();
            outputReceiver.output(new RyeRecommendationDTO(String.valueOf(ryeRecord.get(MEMBERSHIP_NUMBER)), String.valueOf(ryeRecord.get(RYE_PRODUCT_ID)), String.valueOf(ryeRecord.get(LAST_PURCHASE_DATE)), String.valueOf(ryeRecord.get(LAST_PURCHASE_UNITS)), String.valueOf(ryeRecord.get(MODEL_TYPE))));
        }
    }

    public static class RyeCosmosModelToJson extends DoFn<CosmosRyeRecommendationDTO, String> {
        private final Counter ryeCosmosFailureCounter = Metrics.counter(RecommendationTransformer.class, RYE_COSMOS_FAILURE_COUNTER);

        @ProcessElement
        public void processElement(@Element CosmosRyeRecommendationDTO cosmosRyeDto, OutputReceiver<String> outputReceiver) {
            try {
                if (cosmosRyeDto != null) {
                    ryeCosmosFailureCounter.inc();
                    ObjectMapper mapper = ObjectMapperHelper.get();
                    outputReceiver.output(mapper.writeValueAsString(cosmosRyeDto));
                } else {
                    log.error("Unable to Map Element To String");
                }
            } catch (JsonProcessingException je) {
                log.error("Error While Converting Model Object To Json String");
            }

        }
    }

    public static class RyeJsonToCosmosModel extends DoFn<String, CosmosRyeRecommendationDTO> {
        private final Counter ryeCosmosRetryCounter = Metrics.counter(RecommendationTransformer.class, RYE_COSMOS_RETRY_COUNTER);

        @ProcessElement
        public void processElement(@Element String jsonString, OutputReceiver<CosmosRyeRecommendationDTO> outputReceiver) {
            try {
                ryeCosmosRetryCounter.inc();
                ObjectMapper mapper = ObjectMapperHelper.get();
                outputReceiver.output(mapper.readValue(jsonString, CosmosRyeRecommendationDTO.class));
            } catch (JsonProcessingException je) {
                log.error("Error While Parsing error:{}", je.getMessage());
            }
        }
    }
}