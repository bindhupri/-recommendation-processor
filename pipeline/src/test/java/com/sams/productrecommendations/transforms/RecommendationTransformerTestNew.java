package com.sams.productrecommendations.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sams.productrecommendations.config.BatchPipelineOptions;
import com.sams.productrecommendations.model.*;
import com.sams.productrecommendations.transform.RecommendationTransformer;
import com.sams.productrecommendations.utils.RecommendationJunitStubUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.sams.productrecommendations.util.ApplicationConstants.*;

@RunWith(JUnit4.class)
public class RecommendationTransformerTestNew {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(true);

    static {
        // Ensure BatchPipelineOptions is registered
        PipelineOptionsFactory.register(BatchPipelineOptions.class);
    }

    @Test
    public void testProductRecommendationTransformModel() {
        BatchPipelineOptions options = PipelineOptionsFactory.create().as(BatchPipelineOptions.class);
        options.setModule(ValueProvider.StaticValueProvider.of("savings"));
        options.setFeedType(ValueProvider.StaticValueProvider.of("warm-start"));

        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(SAVINGS_PARQUET_SCHEMA));

        PCollection<GenericRecord> inputRecords = pipeline.apply(
                Create.of(RecommendationJunitStubUtils.savingsMembershipGenericRecord())
                        .withCoder(AvroCoder.of(GenericRecord.class, schema))
        );

        PCollection<MembershipRecommendationDTO> outRecords = inputRecords.apply(
                ParDo.of(new RecommendationTransformer.ProductRecommendationModel(options.getModule().get()))
        );

        PAssert.that(outRecords).containsInAnyOrder(
                RecommendationJunitStubUtils.genericProductRecommendationModel(options.getModule().get())
        );

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testLandingColdRecommendationTransformModel() {
        BatchPipelineOptions options = PipelineOptionsFactory.create().as(BatchPipelineOptions.class);
        options.setModule(ValueProvider.StaticValueProvider.of("sng_landing_page"));
        options.setFeedType(ValueProvider.StaticValueProvider.of("cold-start"));

        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(LANDING_PAGE_COLD_PARQUET_SCHEMA));

        PCollection<GenericRecord> inputRecords = pipeline.apply(
                Create.of(RecommendationJunitStubUtils.landingColdMembershipGenericRecord())
                        .withCoder(AvroCoder.of(GenericRecord.class, schema))
        );

        PCollection<LandingColdMembershipRecoModel> outRecords = inputRecords.apply(
                ParDo.of(new RecommendationTransformer.LandingColdProductRecommendationTransform())
        );

        PAssert.that(outRecords).containsInAnyOrder(
                RecommendationJunitStubUtils.genericLandingColdProductRecommendationModel(options.getModule().get())
        );

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testMembershipUUIDRGenericRecord() {
        PCollection<CardHolderRecommendationDTO> inputRecords = pipeline.apply(
                Create.of(RecommendationJunitStubUtils.membershipUUIDRecommendationList())
        );

        PCollection<GenericRecord> outRecords = inputRecords.apply(
                ParDo.of(new RecommendationTransformer.MembershipUUIDToGenericRecord())
        ).setCoder(AvroCoder.of(GenericRecord.class, new Schema.Parser().parse(MEMBERSHIPUUID_AVRO_SCHEMA)));

        PAssert.that(outRecords).containsInAnyOrder(RecommendationJunitStubUtils.membershipGenericRecord());

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRecommendationDtoToGenericRecord() {
        BatchPipelineOptions options = PipelineOptionsFactory.create().as(BatchPipelineOptions.class);
        options.setModule(ValueProvider.StaticValueProvider.of("savings"));
        options.setFeedType(ValueProvider.StaticValueProvider.of("cold-start"));

        PCollection<MembershipRecommendationDTO> inputRecords = pipeline.apply(
                Create.of(RecommendationJunitStubUtils.genericProductRecommendationModel("savings"))
        );

        PCollection<GenericRecord> outRecords = inputRecords.apply(
                ParDo.of(new RecommendationTransformer.RecommendationDtoToGenericRecord())
        ).setCoder(AvroCoder.of(GenericRecord.class, new Schema.Parser().parse(GENERIC_AVRO_SCHEMA)));

        PAssert.that(outRecords).containsInAnyOrder(
                RecommendationJunitStubUtils.savingsMembershipGenericRecordString()
        );

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRyeRecommendationModel() {
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(RYE_PARQUET_SCHEMA));

        PCollection<GenericRecord> inList = pipeline.apply(
                Create.of(RecommendationJunitStubUtils.ryeGenericRecordList())
                        .withCoder(AvroCoder.of(GenericRecord.class, schema))
        );

        PCollection<RyeRecommendationDTO> outList = inList.apply(
                ParDo.of(new RecommendationTransformer.RyeRecommendationModel())
        );

        PAssert.that(outList).containsInAnyOrder(RecommendationJunitStubUtils.ryeRecords());

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRyeCosmosToJsonModel() throws JsonProcessingException {
        PCollection<CosmosRyeRecommendationDTO> inputList = pipeline.apply(
                Create.of(RecommendationJunitStubUtils.ryeCosmosList())
        );

        PCollection<String> outputList = inputList.apply(
                ParDo.of(new RecommendationTransformer.RyeCosmosModelToJson())
        );

        PAssert.that(outputList).containsInAnyOrder(RecommendationJunitStubUtils.ryeCosmosToJson());

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testRyeJsonToCosmosModel() throws JsonProcessingException {
        PCollection<String> inputList = pipeline.apply(
                Create.of(RecommendationJunitStubUtils.ryeCosmosToJson())
        );

        PCollection<CosmosRyeRecommendationDTO> outputList = inputList.apply(
                ParDo.of(new RecommendationTransformer.RyeJsonToCosmosModel())
        );

        PAssert.that(outputList).containsInAnyOrder(RecommendationJunitStubUtils.ryeCosmosList());

        pipeline.run().waitUntilFinish();
    }
}
