package com.sams.productrecommendations.transform;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.sams.productrecommendations.config.BatchPipelineOptions;
import com.sams.productrecommendations.model.CardHolderRecommendationDTO;
import com.sams.productrecommendations.model.CosmosRyeProductDTO;
import com.sams.productrecommendations.model.CosmosRyeProductMetaDataDTO;
import com.sams.productrecommendations.model.CosmosRyeRecommendationDTO;
import com.sams.productrecommendations.model.LandingColdMembershipRecoModel;
import com.sams.productrecommendations.model.MembershipRecommendationDTO;
import com.sams.productrecommendations.model.CosmosProductRecommendationDTO;
import com.sams.productrecommendations.enums.RecommendationType;
import com.sams.productrecommendations.model.RyeRecommendationDTO;
import com.sams.productrecommendations.util.SecretManagerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.ACCOUNY_KEY_SECRET_NAME;
import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_COSMOS_DOCUMENT;
import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_PRODUCT_RECOMMENDATIONS_KVPAIR;
import static com.sams.productrecommendations.util.ApplicationConstants.COMPLEMENTARY_RECOMMENDATION;
import static com.sams.productrecommendations.util.ApplicationConstants.COMSOS_DOCUMENT_AVRO_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_DOCUMENT_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_DOC_GENERIC_RECORD;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_FAILURE_MODEL_TRANSFORM;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_FAILURE_NEW_PARQUET;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_FILE_WRITE_WINDOWING;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_MODEL_TYPE;
import static com.sams.productrecommendations.util.ApplicationConstants.COSMOS_STATEFUL_PROCESSING;
import static com.sams.productrecommendations.util.ApplicationConstants.DIRECT_CONFIG_CONNECT_TIMEOUT;
import static com.sams.productrecommendations.util.ApplicationConstants.HOST_SECRET_NAME;
import static com.sams.productrecommendations.util.ApplicationConstants.ID;
import static com.sams.productrecommendations.util.ApplicationConstants.MAX_RETRY_ATTEMPTS_ON_THROTTLED_REQUESTS;
import static com.sams.productrecommendations.util.ApplicationConstants.MAX_RETRY_WAIT_TIME;
import static com.sams.productrecommendations.util.ApplicationConstants.MODULE_LANDING_PAGE;
import static com.sams.productrecommendations.util.ApplicationConstants.PARQUET_FILE_EXTENSION;
import static com.sams.productrecommendations.util.ApplicationConstants.PERSIST_RECOMMENDATION;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDATION_ID;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDATION_ID_TYPE;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDATION_TYPE_MEMBERSHIP_NBR;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDED_ITEMS;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDED_PRODUCTS;
import static com.sams.productrecommendations.util.ApplicationConstants.RECOMMENDED_UPCS;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_RECOMMENDATION;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_RECOMMENDATION;
import static com.sams.productrecommendations.util.ApplicationConstants.STRATEGY_NAME;
import static com.sams.productrecommendations.util.ApplicationConstants.TTL;
import static com.sams.productrecommendations.util.ApplicationConstants.WRITE_COSMOS_FAILURES_TO_GCS;

@Slf4j
public class CosmosIngestionHelper {

    private CosmosIngestionHelper() {

    }

    public static class BuildCosmosDocumentModel extends PTransform<PCollection<MembershipRecommendationDTO>, PCollection<CosmosProductRecommendationDTO>> {
        String module;

        public BuildCosmosDocumentModel(String module) {
            this.module = module;
        }

        @Override
        public PCollection<CosmosProductRecommendationDTO> expand(PCollection<MembershipRecommendationDTO> input) {
            return input.apply(BUILD_COSMOS_DOCUMENT, ParDo.of(new CosmosDocumentTransformer(getRecommendationTypeByModule(module))));
        }

    }

    public static class BuildLandingColdCosmosDocumentModel extends PTransform<PCollection<LandingColdMembershipRecoModel>, PCollection<CosmosProductRecommendationDTO>> {
        String module;

        public BuildLandingColdCosmosDocumentModel(String module) {
            this.module = module;
        }

        @Override
        public PCollection<CosmosProductRecommendationDTO> expand(PCollection<LandingColdMembershipRecoModel> input) {
            return input.apply(BUILD_COSMOS_DOCUMENT, ParDo.of(new LandingColdCosmosDocumentTransformer(getRecommendationTypeByModule(module))));
        }

    }


    public static class BuildMembershipCosmosDocumentModel extends PTransform<PCollection<CardHolderRecommendationDTO>, PCollection<CosmosProductRecommendationDTO>> {
        String module;

        public BuildMembershipCosmosDocumentModel(String module) {
            this.module = module;
        }

        @Override
        public PCollection<CosmosProductRecommendationDTO> expand(PCollection<CardHolderRecommendationDTO> input) {
            return input.apply(BUILD_COSMOS_DOCUMENT, ParDo.of(new LandingWarmCosmosDocumentTransformer(getRecommendationTypeByModule(module))));
        }
    }

    public static class BuildRyeCosmosDocumentModel extends PTransform<PCollection<RyeRecommendationDTO>, PCollection<CosmosRyeRecommendationDTO>> {

        String module;

        public BuildRyeCosmosDocumentModel(String module) {
            this.module = module;
        }

        @Override
        public PCollection<CosmosRyeRecommendationDTO> expand(PCollection<RyeRecommendationDTO> input) {
            return input.apply(BUILD_COSMOS_DOCUMENT, ParDo.of(new RyeCosmosDocumentTransformer(getRecommendationTypeByModule(module))));
        }
    }


    public static RecommendationType getRecommendationTypeByModule(String module) {
        return switch (module) {
            case SAVINGS_RECOMMENDATION -> RecommendationType.SAVINGS;
            case MODULE_LANDING_PAGE -> RecommendationType.LANDING_PAGE;
            case RYE_RECOMMENDATION -> RecommendationType.RYE;
            case PERSIST_RECOMMENDATION -> RecommendationType.PERSIST;
            case COMPLEMENTARY_RECOMMENDATION -> RecommendationType.COMPLIMENTARY;
            default -> RecommendationType.DEFAULT;
        };
    }

    public static void writeToCosmos(PCollection<CosmosProductRecommendationDTO> productRecommendation,BatchPipelineOptions options) {
        Schema cosmosDocumentAvroSchema = new Schema.Parser().parse(COMSOS_DOCUMENT_AVRO_SCHEMA);
        productRecommendation
                .apply(BUILD_PRODUCT_RECOMMENDATIONS_KVPAIR, ParDo.of(new ProductRecommendationsToKVTransform()))
                .apply(COSMOS_STATEFUL_PROCESSING, ParDo.of(new CosmosIngestionStateFulProcessing(options.getProject(), Integer.parseInt(String.valueOf(options.getCosmosBatchSize())))))
                .apply(COSMOS_DOC_GENERIC_RECORD, ParDo.of(new CosmosDocumentToGenericRecord()))
                .setCoder(AvroCoder.of(GenericRecord.class, cosmosDocumentAvroSchema))
                .apply(COSMOS_FILE_WRITE_WINDOWING, Window.<GenericRecord>into(FixedWindows.of(Duration.standardMinutes(1))).withTimestampCombiner(TimestampCombiner.LATEST))
                .apply(COSMOS_FAILURE_NEW_PARQUET,
                        FileIO.<GenericRecord>write()
                                .via(
                                        ParquetIO.sink(new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(COSMOS_DOCUMENT_PARQUET_SCHEMA)))
                                                .withCompressionCodec(CompressionCodecName.SNAPPY))
                                .to(options.getOutPutFilePath())
                                .withSuffix(PARQUET_FILE_EXTENSION));
    }

    public static void writeRyeToCosmos(BatchPipelineOptions options, PCollection<CosmosRyeRecommendationDTO> productRecommendation) {
         productRecommendation
                .apply(BUILD_PRODUCT_RECOMMENDATIONS_KVPAIR, ParDo.of(new ProductRecommendationsToKVTransformRye()))
                .apply(COSMOS_STATEFUL_PROCESSING, ParDo.of(new CosmosRyeIngestion(options.getProject(), Integer.parseInt(String.valueOf(options.getCosmosBatchSize())))))
                .apply(COSMOS_FAILURE_MODEL_TRANSFORM, ParDo.of(new RecommendationTransformer.RyeCosmosModelToJson()))
                 .apply(WRITE_COSMOS_FAILURES_TO_GCS, TextIO.write().to(options.getOutPutFilePath())
                         .withSuffix(".txt"));
    }

    public static class ProductRecommendationsToKVTransform
            extends DoFn<CosmosProductRecommendationDTO, KV<Integer, CosmosProductRecommendationDTO>> {

        @ProcessElement
        public void processElement(@Element CosmosProductRecommendationDTO productRecommendationsDTO, OutputReceiver<KV<Integer, CosmosProductRecommendationDTO>> out) {
            try {
                out.output(KV.of(ThreadLocalRandom.current().nextInt(10), productRecommendationsDTO));
            } catch (Exception e) {
                log.error("Exception while writeRyeToCosmsos:{}", e.toString());
            }
        }
    }

    public static class ProductRecommendationsToKVTransformRye
            extends DoFn<CosmosRyeRecommendationDTO, KV<Integer, CosmosRyeRecommendationDTO>> {

        @ProcessElement
        public void processElement(@Element CosmosRyeRecommendationDTO productRecommendationsDTO, OutputReceiver<KV<Integer, CosmosRyeRecommendationDTO>> out) {
            try {
                out.output(KV.of(ThreadLocalRandom.current().nextInt(10), productRecommendationsDTO));
            } catch (Exception e) {
                log.error("Exception While KVTransform RYE:{}", e.toString());
            }
        }
    }

    public static class ProductRecommendationsKVToListTransform extends DoFn<KV<Integer, Iterable<CosmosProductRecommendationDTO>>, List<CosmosProductRecommendationDTO>> {
        @ProcessElement
        public void processElement(@Element KV<Integer, Iterable<CosmosProductRecommendationDTO>> productRecommendationsDTOKVs, OutputReceiver<List<CosmosProductRecommendationDTO>> out) {
            try {
                out.output(StreamSupport.stream(Objects.requireNonNull(productRecommendationsDTOKVs.getValue()).spliterator(), false).toList());
            } catch (Exception e) {
                log.error("Exception:{}", e.toString());
            }
        }
    }


    public static class CosmosDocumentTransformer
            extends DoFn<MembershipRecommendationDTO, CosmosProductRecommendationDTO> {

        private final RecommendationType recommendationType;

        public CosmosDocumentTransformer(RecommendationType recommendationType) {
            this.recommendationType = recommendationType;
        }

        @ProcessElement
        public void process(ProcessContext c) {
            MembershipRecommendationDTO membershipRecommendationDTO = c.element();
            c.output(
                    CosmosProductRecommendationDTO.builder()
                            .id(
                                    String.join(
                                            "_",
                                            membershipRecommendationDTO.id(),
                                            recommendationType.getStrategyName(),
                                            membershipRecommendationDTO.modelType()))
                            .recommendationId(membershipRecommendationDTO.id())
                            .recommendationIdType("ClubId")
                            .recommendedProducts(
                                    Arrays.asList(
                                            String.valueOf(membershipRecommendationDTO.recommendations()).split(",")))
                            .ttl(recommendationType.getTtlInSecs())
                            .strategyName(recommendationType.getStrategyName())
                            .modelType(membershipRecommendationDTO.modelType())
                            .build());
        }
    }

    public static class LandingColdCosmosDocumentTransformer
            extends DoFn<LandingColdMembershipRecoModel, CosmosProductRecommendationDTO> {

        private final RecommendationType recommendationType;

        public LandingColdCosmosDocumentTransformer(RecommendationType recommendationType) {
            this.recommendationType = recommendationType;
        }

        @ProcessElement
        public void process(ProcessContext c) {
            LandingColdMembershipRecoModel membershipRecommendationDTO = c.element();
            c.output(
                    CosmosProductRecommendationDTO.builder()
                            .id(
                                    String.join(
                                            "_",
                                            membershipRecommendationDTO.id(),
                                            recommendationType.getStrategyName(),
                                            membershipRecommendationDTO.category(),
                                            membershipRecommendationDTO.modelType()))
                            .recommendationId(membershipRecommendationDTO.id())
                            .recommendationIdType("ClubId")
                            .recommendedProducts(
                                    Arrays.asList(
                                            String.valueOf(membershipRecommendationDTO.recommendations()).split(",")))
                            .ttl(recommendationType.getTtlInSecs())
                            .strategyName(recommendationType.getStrategyName())
                            .modelType(membershipRecommendationDTO.modelType())
                            .categoryId(Long.valueOf(membershipRecommendationDTO.category()))
                            .build());
        }
    }

    public static class LandingWarmCosmosDocumentTransformer
            extends DoFn<CardHolderRecommendationDTO, CosmosProductRecommendationDTO> {

        private final RecommendationType recommendationType;

        public LandingWarmCosmosDocumentTransformer(RecommendationType recommendationType) {
            this.recommendationType = recommendationType;
        }

        @ProcessElement
        public void process(ProcessContext c) {
            CardHolderRecommendationDTO cardHolderRecommendationDTO = c.element();
            c.output(
                    CosmosProductRecommendationDTO.builder()
                            .id(
                                    String.join(
                                            "_",
                                            cardHolderRecommendationDTO.membershipUUID(),
                                            recommendationType.getStrategyName(),
                                            cardHolderRecommendationDTO.modelType()))
                            .recommendationId(cardHolderRecommendationDTO.membershipUUID())
                            .recommendationIdType("MembershipUUID")
                            .recommendedProducts(
                                    Arrays.asList(
                                            String.valueOf(cardHolderRecommendationDTO.rankedProducts()).split(",")))
                            .ttl(recommendationType.getTtlInSecs())
                            .strategyName(recommendationType.getStrategyName())
                            .modelType(cardHolderRecommendationDTO.modelType())
                            .build());
        }
    }


    public static class RyeCosmosDocumentTransformer
            extends DoFn<RyeRecommendationDTO, CosmosRyeRecommendationDTO> {

        private final RecommendationType recommendationType;

        public RyeCosmosDocumentTransformer(RecommendationType recommendationType) {
            this.recommendationType = recommendationType;
        }

        @ProcessElement
        public void process(@Element RyeRecommendationDTO ryeRecommendationDTO, OutputReceiver<CosmosRyeRecommendationDTO> out) {
            out.output(
                    CosmosRyeRecommendationDTO.builder()
                            .id(
                                    String.join(
                                            "_",
                                            ryeRecommendationDTO.membershipNumber(),
                                            recommendationType.getStrategyName(),
                                            ryeRecommendationDTO.model()))
                            .recommendationId(ryeRecommendationDTO.membershipNumber())
                            .recommendationIdType(RECOMMENDATION_TYPE_MEMBERSHIP_NBR)
                            .recommendedProductsV1(buildRyeRecommendationModel(ryeRecommendationDTO))
                            .ttl(recommendationType.getTtlInSecs())
                            .strategyName(recommendationType.getStrategyName())
                            .modelType(ryeRecommendationDTO.model())
                            .build());
        }

        public List<CosmosRyeProductDTO> buildRyeRecommendationModel(RyeRecommendationDTO ryeRecommendationDTO) {
            List<String> productRecommendations = Arrays.asList(ryeRecommendationDTO.productIds().split(","));
            List<String> lastPurchasedDate = Arrays.asList(ryeRecommendationDTO.lastPurchaseDate().split(","));
            List<String> lastPurchasedQuantity = Arrays.asList(ryeRecommendationDTO.lastPurchaseUnits().split(","));
            List<CosmosRyeProductDTO> cosmosRyeProductList = new ArrayList<>();
            try {
                cosmosRyeProductList = IntStream.range(0, productRecommendations.size())
                        .mapToObj(i -> new CosmosRyeProductDTO(productRecommendations.get(i), new CosmosRyeProductMetaDataDTO(lastPurchasedDate.get(i), lastPurchasedQuantity.get(i)))).toList();
            } catch (IndexOutOfBoundsException ie) {
                log.error("Error While Reading Products:{} LastPurchaseQuantity:{} LastPurchasedDate:{} Error:{}", productRecommendations, lastPurchasedQuantity, lastPurchasedDate, ie.getMessage());
            }
            return cosmosRyeProductList;

        }

    }


    public static class CosmosDocumentToGenericRecord
            extends DoFn<CosmosProductRecommendationDTO, GenericRecord> {

        @ProcessElement
        public void process(ProcessContext c) {
            GenericRecord genericRecord =
                    new GenericData.Record(new Schema.Parser().parse(COMSOS_DOCUMENT_AVRO_SCHEMA));
            genericRecord.put(ID, requireNonNull(c.element().id()));
            genericRecord.put(RECOMMENDATION_ID, requireNonNull(c.element().recommendationId()));
            genericRecord.put(RECOMMENDATION_ID_TYPE, requireNonNull(c.element().recommendationIdType()));
            genericRecord.put(STRATEGY_NAME, requireNonNull(c.element().strategyName()));
            genericRecord.put(COSMOS_MODEL_TYPE, requireNonNull(c.element().modelType()));
            genericRecord.put(RECOMMENDED_PRODUCTS, String.join(",", requireNonNullList(c.element().recommendedProducts())));
            genericRecord.put(RECOMMENDED_ITEMS, requireNonNullList(c.element().recommendedItems()));
            genericRecord.put(RECOMMENDED_UPCS, String.join(",", requireNonNullList(c.element().recommendedUPCs())));
            genericRecord.put(TTL, 1);
            c.output(genericRecord);
        }

        public String requireNonNull(String input) {
            return Objects.requireNonNull(input);
        }

        public String requireNonNullList(List<String> input) {
            return input != null && !input.isEmpty() ? input.toString() : "";
        }
    }


    public static CosmosAsyncClient buildCosmosAsyncClient(String projectId) {
        var retryOptions = new ThrottlingRetryOptions();
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(MAX_RETRY_ATTEMPTS_ON_THROTTLED_REQUESTS);
        retryOptions.setMaxRetryWaitTime(java.time.Duration.ofSeconds(MAX_RETRY_WAIT_TIME));
        var directConnectionConfig = DirectConnectionConfig.getDefaultConfig();
        directConnectionConfig.setConnectTimeout(java.time.Duration.ofMinutes(DIRECT_CONFIG_CONNECT_TIMEOUT));
        return
                new CosmosClientBuilder()
                        .endpoint(SecretManagerUtil.getSecret(projectId, HOST_SECRET_NAME))
                        .key(SecretManagerUtil.getSecret(projectId, ACCOUNY_KEY_SECRET_NAME))
                        .throttlingRetryOptions(retryOptions)
                        .preferredRegions(Collections.emptyList())
                        .consistencyLevel(ConsistencyLevel.CONSISTENT_PREFIX)
                        .multipleWriteRegionsEnabled(Boolean.FALSE)
                        .clientTelemetryEnabled(Boolean.FALSE)
                        .contentResponseOnWriteEnabled(true)
                        .userAgentSuffix("")
                        .directMode(directConnectionConfig)
                        .buildAsyncClient();
    }
}
