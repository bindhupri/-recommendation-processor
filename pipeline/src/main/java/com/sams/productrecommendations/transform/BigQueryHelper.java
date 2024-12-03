package com.sams.productrecommendations.transform;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import com.sams.productrecommendations.model.MembershipCardHolderDTO;
import com.sams.productrecommendations.model.CardHolderRecommendationDTO;
import com.sams.productrecommendations.model.MembershipRecommendationDTO;
import com.sams.productrecommendations.util.QueryConstants;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;

import static com.sams.productrecommendations.util.ApplicationConstants.AUDIT;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_CARD_NUMBER;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_DATA_TYPE_STRING;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_MEMBERSHIP_ID;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_MEMBERSHIP_UUID;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_MODEL_TYPE;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_RANKED_PRODUCTS;
import static com.sams.productrecommendations.util.ApplicationConstants.BIGQUERY_REQUIRED;
import static com.sams.productrecommendations.util.ApplicationConstants.GET_UUID_FOR_MEMBERSHIP;
import static com.sams.productrecommendations.util.ApplicationConstants.MAP_BIGQUERY_MEMBERSHIP_RECOMM_MODEL;
import static com.sams.productrecommendations.util.ApplicationConstants.MODULE_LANDING_PAGE;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_RECOMMENDATION;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_RECOMMENDATION;

@Slf4j
public class BigQueryHelper {

    private BigQueryHelper() {

    }

    public static class WriteToBigquery extends PTransform<PCollection<MembershipRecommendationDTO>, WriteResult> {

        ValueProvider<String> module;

        String projectId;

        public WriteToBigquery(ValueProvider<String> module, String projectId) {
            this.module = module;
            this.projectId = projectId;
        }

        @Override
        public WriteResult expand(PCollection<MembershipRecommendationDTO> input) {

            // Define a table schema. A schema is required for write disposition CREATE_IF_NEEDED.
            TableSchema schema = new TableSchema()
                    .setFields(
                            Arrays.asList(
                                    new TableFieldSchema()
                                            .setName(BIGQUERY_CARD_NUMBER)
                                            .setType(BIGQUERY_DATA_TYPE_STRING)
                                            .setMode(BIGQUERY_REQUIRED),
                                    new TableFieldSchema()
                                            .setName(BIGQUERY_RANKED_PRODUCTS)
                                            .setType(BIGQUERY_DATA_TYPE_STRING)
                                            .setMode(BIGQUERY_REQUIRED),
                                    new TableFieldSchema()
                                            .setName(BIGQUERY_MODEL_TYPE)
                                            .setType(BIGQUERY_DATA_TYPE_STRING)
                                            .setMode(BIGQUERY_REQUIRED)
                            )
                    );

            final Clustering clustering = new Clustering().setFields(Arrays.asList(BIGQUERY_CARD_NUMBER, BIGQUERY_RANKED_PRODUCTS, BIGQUERY_MODEL_TYPE));
            return input.apply(BigQueryIO.<MembershipRecommendationDTO>write()
                    .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                    .to(String.format("%s:%s.%s", projectId, getDataSetName(String.valueOf(module)), getTableName(String.valueOf(module))))
                    .withFormatFunction(
                            (MembershipRecommendationDTO membershipRecord) -> new TableRow()
                                    .set(BIGQUERY_CARD_NUMBER, Objects.requireNonNull(membershipRecord).id())
                                    .set(BIGQUERY_RANKED_PRODUCTS, membershipRecord.recommendations())
                                    .set(BIGQUERY_MODEL_TYPE, membershipRecord.modelType()))
                    .withSchema(schema)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                    .withClustering(clustering));
        }
    }


    public static PCollection<CardHolderRecommendationDTO> readFromBigQuery(String projectId, Pipeline pipeline, String module) {
        return pipeline.apply(GET_UUID_FOR_MEMBERSHIP,
                        BigQueryIO.read(
                                        (SchemaAndRecord schemaRecord) ->
                                                new MembershipCardHolderDTO(
                                                        String.valueOf(Objects.requireNonNull(schemaRecord).getRecord().get(BIGQUERY_CARD_NUMBER)).trim(),
                                                        String.valueOf(schemaRecord.getRecord().get(BIGQUERY_MEMBERSHIP_ID)).trim(),
                                                        String.valueOf(schemaRecord.getRecord().get(BIGQUERY_MEMBERSHIP_UUID)).trim(),
                                                        String.valueOf(schemaRecord.getRecord().get(BIGQUERY_MODEL_TYPE)).trim(),
                                                        String.valueOf(schemaRecord.getRecord().get(BIGQUERY_RANKED_PRODUCTS)).trim()))
                                .fromQuery(String.format(getQueryByModule(module), projectId))
                                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                                .withoutValidation()
                                .usingStandardSql()
                                .withCoder(SerializableCoder.of(MembershipCardHolderDTO.class)))
                .apply(MAP_BIGQUERY_MEMBERSHIP_RECOMM_MODEL, ParDo.of(new RecommendationTransformer.BigQueryMembershipModelToUUIDModel()));
    }

    public static String getDataSetName(String module) {
        return switch (module) {
            case SAVINGS_RECOMMENDATION -> "temp_savings";
            case MODULE_LANDING_PAGE -> "temp_landing_page";
            case RYE_RECOMMENDATION -> "temp_rye";
            case AUDIT -> "audit";
            default -> "temp_default";
        };
    }

    public static String getTableName(String module) {
        return switch (module) {
            case SAVINGS_RECOMMENDATION -> "SAVINGS_RECOMMENDATION";
            case MODULE_LANDING_PAGE -> "LANDING_PAGE_RECOMMENDATION";
            case RYE_RECOMMENDATION -> "RYE_RECOMMENDATION";
            case AUDIT -> "recommendations-audit";
            default -> "DEFAULT_RECOMMENDATION";
        };
    }

    public static String getQueryByModule(String module) {
        return switch (module) {
            case SAVINGS_RECOMMENDATION -> QueryConstants.SAVINGS_JOIN_QUERY;
            case MODULE_LANDING_PAGE -> QueryConstants.LANDING_PAGE_QUERY;
            case RYE_RECOMMENDATION -> QueryConstants.RYE_QUERY;
            default -> QueryConstants.DEFAULT_JOIN_QUERY;
        };
    }

}