package com.sams.productrecommendations.transform;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import com.sams.productrecommendations.config.BatchPipelineOptions;
import com.sams.productrecommendations.model.AuditRecommendationDTO;
import com.sams.productrecommendations.model.CosmosProductRecommendationDTO;
import com.sams.productrecommendations.model.CardHolderRecommendationDTO;
import com.sams.productrecommendations.model.LandingColdMembershipRecoModel;
import com.sams.productrecommendations.model.MembershipRecommendationDTO;
import com.sams.productrecommendations.model.RyeRecommendationDTO;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.transform.BigQueryHelper.getDataSetName;
import static com.sams.productrecommendations.transform.BigQueryHelper.getTableName;
import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT;
import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT_BIGQUERY_WRITE;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_COLUMN_FEED_TYPE;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_COLUMN_PROCESSED_TIME;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_COLUMN_RECORD_COUNT;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_COLUMN_RECO_TYPE;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_COLUMN_RECO_UUID;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_COLUMN_STATUS;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_DATA_TYPE_NUMERIC;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_DATA_TYPE_STRING;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_NULLABLE;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_REQUIRED;
import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_AUDIT_MODEL;
import static com.sams.productrecommendations.util.ApplicationConstants.COUNT_RECORDS;

@Slf4j
public class AuditTransformer {

    private AuditTransformer() {

    }

    public static class AuditParquetRead extends PTransform<PCollection<GenericRecord>, PCollection<GenericRecord>> {
        String status;
        String projectId;

        public AuditParquetRead(String status, String projectId) {
            this.status = status;
            this.projectId = projectId;
        }

        @Override
        public PCollection<GenericRecord> expand(PCollection<GenericRecord> input) {
            input.apply(COUNT_RECORDS, Count.globally())
                    .apply(AUDIT_BIGQUERY_WRITE, new AuditBigQueryWrite(projectId, status));
            return input;
        }
    }

    public static class AuditRecommendationModel extends PTransform<PCollection<MembershipRecommendationDTO>, PCollection<MembershipRecommendationDTO>> {
        String status;
        String projectId;

        public AuditRecommendationModel(String status, String projectId) {
            this.status = status;
            this.projectId = projectId;
        }

        @Override
        public PCollection<MembershipRecommendationDTO> expand(PCollection<MembershipRecommendationDTO> input) {
            input.apply(COUNT_RECORDS, Count.globally())
                    .apply(AUDIT_BIGQUERY_WRITE, new AuditBigQueryWrite(projectId, status));
            return input;
        }
    }

    public static class AuditLandingColdRecommendationModel extends PTransform<PCollection<LandingColdMembershipRecoModel>, PCollection<LandingColdMembershipRecoModel>> {
        String status;
        String projectId;

        public AuditLandingColdRecommendationModel(String status, String projectId) {
            this.status = status;
            this.projectId = projectId;
        }

        @Override
        public PCollection<LandingColdMembershipRecoModel> expand(PCollection<LandingColdMembershipRecoModel> input) {
            input.apply(COUNT_RECORDS, Count.globally())
                    .apply(AUDIT_BIGQUERY_WRITE, new AuditBigQueryWrite(projectId, status));
            return input;
        }
    }

    public static class AuditRyeRecommendationModel extends PTransform<PCollection<RyeRecommendationDTO>, PCollection<RyeRecommendationDTO>> {
        String status;
        String projectId;

        public AuditRyeRecommendationModel(String status, String projectId) {
            this.status = status;
            this.projectId = projectId;
        }

        @Override
        public PCollection<RyeRecommendationDTO> expand(PCollection<RyeRecommendationDTO> input) {
            input.apply(COUNT_RECORDS, Count.globally())
                    .apply(AUDIT_BIGQUERY_WRITE, new AuditBigQueryWrite(projectId, status));
            return input;
        }
    }

    public static class AuditBigQueryRead extends PTransform<PCollection<CardHolderRecommendationDTO>, PCollection<CardHolderRecommendationDTO>> {
        String status;
        String projectId;

        public AuditBigQueryRead(String status, String projectId) {
            this.status = status;
            this.projectId = projectId;
        }

        @Override
        public PCollection<CardHolderRecommendationDTO> expand(PCollection<CardHolderRecommendationDTO> input) {
            input.apply(COUNT_RECORDS, Count.globally())
                    .apply(AUDIT_BIGQUERY_WRITE, new AuditBigQueryWrite(projectId, status));
            return input;
        }
    }

    public static class AuditCosmosWrite extends PTransform<PCollection<CosmosProductRecommendationDTO>, PCollection<CosmosProductRecommendationDTO>> {
        String status;
        String projectId;

        public AuditCosmosWrite(String status, String projectId) {
            this.status = status;
            this.projectId = projectId;
        }

        @Override
        public PCollection<CosmosProductRecommendationDTO> expand(PCollection<CosmosProductRecommendationDTO> input) {
            input.apply(COUNT_RECORDS, Count.globally())
                    .apply(AUDIT_BIGQUERY_WRITE, new AuditBigQueryWrite(projectId, status));
            return input;
        }
    }

    public static class AuditRyeCosmosFailures extends PTransform<PCollection<String>, PCollection<String>> {
        String status;
        String projectId;

        public AuditRyeCosmosFailures(String status, String projectId) {
            this.status = status;
            this.projectId = projectId;
        }

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            input.apply(COUNT_RECORDS, Count.globally())
                    .apply(AUDIT_BIGQUERY_WRITE, new AuditBigQueryWrite(projectId, status));
            return input;
        }
    }

    public static class AuditModelTransform extends DoFn<Long, AuditRecommendationDTO> {
        String status;

        public AuditModelTransform(String status) {
            this.status = status;
        }

        @ProcessElement
        public void processContext(ProcessContext pc, @Element Long count) {
            pc.output(new AuditRecommendationDTO(
                    String.valueOf(pc.getPipelineOptions().as(BatchPipelineOptions.class).getUuidToAudit()),
                    String.valueOf(pc.getPipelineOptions().as(BatchPipelineOptions.class).getModule()),
                    String.valueOf(pc.getPipelineOptions().as(BatchPipelineOptions.class).getFeedType()),
                    status,
                    String.valueOf(Instant.now()),
                    count
            ));
        }
    }

    public static class AuditBigQueryWrite extends PTransform<PCollection<Long>, WriteResult> {

        String projectId;
        String status;

        public AuditBigQueryWrite(String projectId, String status) {
            this.projectId = projectId;
            this.status = status;
        }

        // Define a table schema. A schema is required for write disposition CREATE_IF_NEEDED.
        private final transient TableSchema schema = new TableSchema()
                .setFields(
                        Arrays.asList(
                                new TableFieldSchema()
                                        .setName(BIGQUERY_COLUMN_RECO_UUID)
                                        .setType(BIGQUERY_DATA_TYPE_STRING)
                                        .setMode(BIGQUERY_REQUIRED),
                                new TableFieldSchema()
                                        .setName(BIGQUERY_COLUMN_RECO_TYPE)
                                        .setType(BIGQUERY_DATA_TYPE_STRING)
                                        .setMode(BIGQUERY_REQUIRED),
                                new TableFieldSchema()
                                        .setName(BIGQUERY_COLUMN_FEED_TYPE)
                                        .setType(BIGQUERY_DATA_TYPE_STRING)
                                        .setMode(BIGQUERY_NULLABLE),
                                new TableFieldSchema()
                                        .setName(BIGQUERY_COLUMN_RECORD_COUNT)
                                        .setType(BIGQUERY_DATA_TYPE_NUMERIC)
                                        .setMode(BIGQUERY_REQUIRED),
                                new TableFieldSchema()
                                        .setName(BIGQUERY_COLUMN_STATUS)
                                        .setType(BIGQUERY_DATA_TYPE_STRING)
                                        .setMode(BIGQUERY_REQUIRED),
                                new TableFieldSchema()
                                        .setName(BIGQUERY_COLUMN_PROCESSED_TIME)
                                        .setType(BIGQUERY_DATA_TYPE_STRING)
                                        .setMode(BIGQUERY_REQUIRED)
                        )
                );


        @Override
        public WriteResult expand(PCollection<Long> input) {
            return input.apply(BUILD_AUDIT_MODEL, ParDo.of(new AuditModelTransform(status)))
                    .apply(BigQueryIO.<AuditRecommendationDTO>write()
                            .to(String.format("%s:%s.%s", projectId, getDataSetName(AUDIT), getTableName(AUDIT)))
                            .withFormatFunction(
                                    (AuditRecommendationDTO auditRecommendationDTO) -> new TableRow()
                                            .set(BIGQUERY_COLUMN_RECO_UUID, Objects.requireNonNull(auditRecommendationDTO).uuid())
                                            .set(BIGQUERY_COLUMN_RECO_TYPE, Objects.requireNonNull(auditRecommendationDTO.recommendationType()))
                                            .set(BIGQUERY_COLUMN_FEED_TYPE, auditRecommendationDTO.feedType())
                                            .set(BIGQUERY_COLUMN_RECORD_COUNT, Objects.requireNonNull(auditRecommendationDTO).count())
                                            .set(BIGQUERY_COLUMN_STATUS, Objects.requireNonNull(auditRecommendationDTO).status())
                                            .set(BIGQUERY_COLUMN_PROCESSED_TIME, Objects.requireNonNull(auditRecommendationDTO).processedTime()))
                            .withSchema(schema)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        }
    }

}
