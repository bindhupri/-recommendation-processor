package com.sams.productrecommendations.batch;

import com.sams.productrecommendations.config.BatchPipelineOptions;
import com.sams.productrecommendations.model.CardHolderRecommendationDTO;
import com.sams.productrecommendations.model.CosmosProductRecommendationDTO;
import com.sams.productrecommendations.model.MembershipRecommendationDTO;
import com.sams.productrecommendations.transform.AuditTransformer;
import com.sams.productrecommendations.transform.BigQueryHelper;
import com.sams.productrecommendations.transform.CosmosIngestionHelper;
import com.sams.productrecommendations.transform.FileWriteTransformer;
import com.sams.productrecommendations.transform.RecommendationTransformer;
import com.sams.productrecommendations.util.SchemaUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_BIGQUERY_READ;
import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_COLD_DB_WRITE;
import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_FILE_READ;
import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_MODEL_CONSTRUCTION;
import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_WARM_DB_WRITE;
import static com.sams.productrecommendations.util.ApplicationConstants.BIG_QUERY_WRITE_TRANSFORM;
import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_COSMOS_DOCUMENT;
import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_RECOMM_MODEL;
import static com.sams.productrecommendations.util.ApplicationConstants.MEMBERSHIPUUID_AVRO_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.MEM_UUID_GENERIC;
import static com.sams.productrecommendations.util.ApplicationConstants.OPTIONS_FLAG_Y;
import static com.sams.productrecommendations.util.ApplicationConstants.READ_PARQUET_FILE;
import static com.sams.productrecommendations.util.ApplicationConstants.RECO_TO_GENERIC;
import static com.sams.productrecommendations.util.ApplicationConstants.WRITE_TO_CLOUD_STORAGE;

@Slf4j
public class GenericRecommendationProcessor {

    public void start(BatchPipelineOptions options, Pipeline pipeline) {
        if (OPTIONS_FLAG_Y.equalsIgnoreCase(String.valueOf(options.getCardHolderRecommendation()))) {
            processRecommendationsFromBigQuery(options, pipeline);
        } else {
            processRecommendationsFromFile(options, pipeline);
        }
    }

    void processRecommendationsFromBigQuery(BatchPipelineOptions options, Pipeline pipeline) {
        Schema membershipSchema = new Schema.Parser().parse(MEMBERSHIPUUID_AVRO_SCHEMA);
        PCollection<CardHolderRecommendationDTO> bigQueryCollection = BigQueryHelper.readFromBigQuery(options.getProject(), pipeline, String.valueOf(options.getModule()))
                .apply(AUDIT_STATUS_BIGQUERY_READ, new AuditTransformer.AuditBigQueryRead(AUDIT_STATUS_BIGQUERY_READ, options.getProject()));

        if (OPTIONS_FLAG_Y.equalsIgnoreCase(String.valueOf(options.getWriteToCloudStorage()))) {
            bigQueryCollection
                    .apply(MEM_UUID_GENERIC, ParDo.of(new RecommendationTransformer.MembershipUUIDToGenericRecord())).setCoder(AvroCoder.of(GenericRecord.class, membershipSchema))
                    .apply(WRITE_TO_CLOUD_STORAGE, new FileWriteTransformer(options.getModule(), options.getOutPutFilePath()));
        }

        if (OPTIONS_FLAG_Y.equalsIgnoreCase(String.valueOf(options.getWriteToCosmos()))) {
            PCollection<CosmosProductRecommendationDTO> cosmosIngestDocument = bigQueryCollection
                    .apply(BUILD_COSMOS_DOCUMENT, new CosmosIngestionHelper.BuildMembershipCosmosDocumentModel(String.valueOf(options.getModule())))
                    .apply(AUDIT_STATUS_WARM_DB_WRITE, new AuditTransformer.AuditCosmosWrite(AUDIT_STATUS_WARM_DB_WRITE, options.getProject()));
            CosmosIngestionHelper.writeToCosmos(cosmosIngestDocument, options);
        }
        pipeline.run().waitUntilFinish();
    }


    void processRecommendationsFromFile(BatchPipelineOptions options, Pipeline pipeline) {
        PCollection<MembershipRecommendationDTO> productRecommendation =
                pipeline.apply(READ_PARQUET_FILE, ParquetIO.read(SchemaUtils.getSchemaByFeedType(String.valueOf(options.getModule()),String.valueOf(options.getFeedType()))).from(options.getInputFile()))
                        .apply(AUDIT_STATUS_FILE_READ, new AuditTransformer.AuditParquetRead(AUDIT_STATUS_FILE_READ, options.getProject()))
                        .apply(BUILD_RECOMM_MODEL, ParDo.of(new RecommendationTransformer.ProductRecommendationModel(String.valueOf(options.getModule()))))
                        .apply(AUDIT_STATUS_MODEL_CONSTRUCTION, new AuditTransformer.AuditRecommendationModel(AUDIT_STATUS_MODEL_CONSTRUCTION,
                                options.getProject()));

        if (OPTIONS_FLAG_Y.equalsIgnoreCase(String.valueOf(options.getWriteToBigQuery()))) {
            writeToBigQuery(productRecommendation, options);
        }

        if (OPTIONS_FLAG_Y.equalsIgnoreCase(String.valueOf(options.getWriteToCloudStorage()))) {
            writeToCloudStorage(productRecommendation, options);
        }

        if (OPTIONS_FLAG_Y.equalsIgnoreCase(String.valueOf(options.getWriteToCosmos()))) {
            writeToCosmos(productRecommendation, options);
        }
        pipeline.run().waitUntilFinish();
    }

    void writeToBigQuery(PCollection<MembershipRecommendationDTO> productRecommendation, BatchPipelineOptions options) {
        productRecommendation
                .apply(BIG_QUERY_WRITE_TRANSFORM, new BigQueryHelper.WriteToBigquery(options.getModule(), options.getProject()));
    }

    void writeToCloudStorage(PCollection<MembershipRecommendationDTO> productRecommendation, BatchPipelineOptions options) {
        Schema membershipSchema = new Schema.Parser().parse(MEMBERSHIPUUID_AVRO_SCHEMA);
        productRecommendation
                .apply(RECO_TO_GENERIC, ParDo.of(new RecommendationTransformer.RecommendationDtoToGenericRecord())).setCoder(AvroCoder.of(GenericRecord.class, membershipSchema))
                .apply(WRITE_TO_CLOUD_STORAGE, new FileWriteTransformer(options.getModule(), options.getOutPutFilePath()));
    }

    void writeToCosmos(PCollection<MembershipRecommendationDTO> productRecommendation, BatchPipelineOptions options) {
        PCollection<CosmosProductRecommendationDTO> writeToCosmosWarm = productRecommendation
                .apply(BUILD_COSMOS_DOCUMENT, new CosmosIngestionHelper.BuildCosmosDocumentModel(String.valueOf(options.getModule())))
                .apply(AUDIT_STATUS_COLD_DB_WRITE, new AuditTransformer.AuditCosmosWrite(AUDIT_STATUS_COLD_DB_WRITE, options.getProject()));
        CosmosIngestionHelper.writeToCosmos(writeToCosmosWarm, options);
    }
}
