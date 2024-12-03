package com.sams.productrecommendations.batch;

import com.sams.productrecommendations.config.BatchPipelineOptions;
import com.sams.productrecommendations.model.CosmosProductRecommendationDTO;
import com.sams.productrecommendations.model.LandingColdMembershipRecoModel;
import com.sams.productrecommendations.model.MembershipRecommendationDTO;
import com.sams.productrecommendations.transform.AuditTransformer;
import com.sams.productrecommendations.transform.CosmosIngestionHelper;
import com.sams.productrecommendations.transform.RecommendationTransformer;
import com.sams.productrecommendations.util.SchemaUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_COLD_DB_WRITE;
import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_FILE_READ;
import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_MODEL_CONSTRUCTION;
import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_COSMOS_DOCUMENT;
import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_RECOMM_MODEL;
import static com.sams.productrecommendations.util.ApplicationConstants.FEED_TYPE_COLD;
import static com.sams.productrecommendations.util.ApplicationConstants.OPTIONS_FLAG_Y;
import static com.sams.productrecommendations.util.ApplicationConstants.READ_PARQUET_FILE;

@Slf4j
public class LandingPageRecommendationProcessor extends GenericRecommendationProcessor {

    @Override
    public void start(BatchPipelineOptions options, Pipeline pipeline) {
        if (OPTIONS_FLAG_Y.equalsIgnoreCase(String.valueOf(options.getCardHolderRecommendation()))) {
            processRecommendationsFromBigQuery(options, pipeline);
        } else {
            processRecommendationsFromFile(options, pipeline);
        }
    }

    @Override
    void processRecommendationsFromFile(BatchPipelineOptions options, Pipeline pipeline) {
        PCollection<GenericRecord> genericRecordCollection = pipeline.apply(READ_PARQUET_FILE, ParquetIO.read(SchemaUtils.getSchemaByFeedType(String.valueOf(options.getModule()), String.valueOf(options.getFeedType()))).from(options.getInputFile()))
                .apply(AUDIT_STATUS_FILE_READ, new AuditTransformer.AuditParquetRead(AUDIT_STATUS_FILE_READ, options.getProject()));
        if (String.valueOf(options.getFeedType()).contains(FEED_TYPE_COLD)) {
            PCollection<LandingColdMembershipRecoModel> productRecommendation = genericRecordCollection.apply(BUILD_RECOMM_MODEL, ParDo.of(new RecommendationTransformer.LandingColdProductRecommendationTransform()))
                    .apply(AUDIT_STATUS_MODEL_CONSTRUCTION, new AuditTransformer.AuditLandingColdRecommendationModel(AUDIT_STATUS_MODEL_CONSTRUCTION,
                            options.getProject()));
            if (OPTIONS_FLAG_Y.equalsIgnoreCase(String.valueOf(options.getWriteToCosmos()))) {
                writeToLandingCosmosCold(productRecommendation, options);
            }

        } else {
            PCollection<MembershipRecommendationDTO> productRecommendation = genericRecordCollection.apply(BUILD_RECOMM_MODEL, ParDo.of(new RecommendationTransformer.ProductRecommendationModel(String.valueOf(options.getModule()))))
                    .apply(AUDIT_STATUS_MODEL_CONSTRUCTION, new AuditTransformer.AuditRecommendationModel(AUDIT_STATUS_MODEL_CONSTRUCTION,
                            options.getProject()));

            if (OPTIONS_FLAG_Y.equalsIgnoreCase(String.valueOf(options.getWriteToBigQuery()))) {
                writeToBigQuery(productRecommendation, options);
            }

        }
        pipeline.run().waitUntilFinish();
    }

    void writeToLandingCosmosCold(PCollection<LandingColdMembershipRecoModel> productRecommendation, BatchPipelineOptions options) {
        PCollection<CosmosProductRecommendationDTO> writeToCosmosCold = productRecommendation
                .apply(BUILD_COSMOS_DOCUMENT, new CosmosIngestionHelper.BuildLandingColdCosmosDocumentModel(String.valueOf(options.getModule())))
                .apply(AUDIT_STATUS_COLD_DB_WRITE, new AuditTransformer.AuditCosmosWrite(AUDIT_STATUS_COLD_DB_WRITE, options.getProject()));
        CosmosIngestionHelper.writeToCosmos(writeToCosmosCold, options);
    }

}
