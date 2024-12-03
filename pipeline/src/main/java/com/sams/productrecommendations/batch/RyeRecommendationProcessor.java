package com.sams.productrecommendations.batch;

import com.sams.productrecommendations.config.BatchPipelineOptions;
import com.sams.productrecommendations.model.CosmosRyeRecommendationDTO;
import com.sams.productrecommendations.transform.AuditTransformer;
import com.sams.productrecommendations.transform.CosmosIngestionHelper;
import com.sams.productrecommendations.transform.RecommendationTransformer;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_FILE_READ;
import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_STATUS_MODEL_CONSTRUCTION;
import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_COSMOS_DOCUMENT;
import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_RECOMM_MODEL;
import static com.sams.productrecommendations.util.ApplicationConstants.READ_PARQUET_FILE;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_PARQUET_SCHEMA;


@Slf4j
public class RyeRecommendationProcessor {

    private RyeRecommendationProcessor() {
    }

    public static void processRecommendationsFromFile(BatchPipelineOptions options, Pipeline pipeline) {
        PCollection<CosmosRyeRecommendationDTO> cosmosCollection = pipeline.apply(READ_PARQUET_FILE, ParquetIO.read(new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(RYE_PARQUET_SCHEMA))).from(options.getInputFile()))
                .apply(AUDIT_STATUS_FILE_READ, new AuditTransformer.AuditParquetRead(AUDIT_STATUS_FILE_READ, options.getProject()))
                .apply(BUILD_RECOMM_MODEL, ParDo.of(new RecommendationTransformer.RyeRecommendationModel()))
                .apply(AUDIT_STATUS_MODEL_CONSTRUCTION, new AuditTransformer.AuditRyeRecommendationModel(AUDIT_STATUS_MODEL_CONSTRUCTION, options.getProject()))
                .apply(BUILD_COSMOS_DOCUMENT, new CosmosIngestionHelper.BuildRyeCosmosDocumentModel(String.valueOf(options.getModule())));
        CosmosIngestionHelper.writeRyeToCosmos(options, cosmosCollection);
        pipeline.run().waitUntilFinish();
    }
}
