package com.sams.productrecommendations.transforms;

import com.sams.productrecommendations.model.MembershipRecommendationDTO;
import com.sams.productrecommendations.transform.RecommendationTransformer;
import org.apache.avro.Schema;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineResult;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Rule;
import org.junit.Test;

import static com.sams.productrecommendations.util.ApplicationConstants.*;

import static org.junit.Assert.assertEquals;

public class ProductRecommendationModelTest {

    @Rule
    public final TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testProcessRecord() {
        // Mock schema definition
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(SAVINGS_PARQUET_SCHEMA));
        GenericRecord mockRecord1 = new GenericData.Record(schema);
        mockRecord1.put(SOURCE_ID, "123");
        mockRecord1.put(RANKED_PRODUCTS, "productA, productB");
        mockRecord1.put(MODEL_TYPE, "savings");

        GenericRecord mockRecord2 = new GenericData.Record(schema);
        mockRecord2.put(SOURCE_ID, "456");
        mockRecord2.put(RANKED_PRODUCTS, "productC");
        mockRecord2.put(MODEL_TYPE, "landing");

        // Create TestStream of GenericRecord
        TestStream<GenericRecord> testStream = TestStream.create(AvroCoder.of(GenericRecord.class, schema))
                .addElements(mockRecord1, mockRecord2)
                .advanceWatermarkToInfinity();

        // Apply the ParDo to the pipeline
        PCollection<MembershipRecommendationDTO> output = pipeline
                .apply(testStream)
                .apply(ParDo.of(new RecommendationTransformer.ProductRecommendationModel(SAVINGS_RECOMMENDATION_COUNTER)));

        // Verify the output
        MembershipRecommendationDTO expectedOutput1 = new MembershipRecommendationDTO("123", "productA, productB", "savings");
        MembershipRecommendationDTO expectedOutput2 = new MembershipRecommendationDTO("456", "productC", "landing");

        PAssert.that(output)
                .containsInAnyOrder(expectedOutput1, expectedOutput2);

        // Run the pipeline and capture the result
        PipelineResult result = pipeline.run();
        result.waitUntilFinish();

        // Verify metrics
        MetricQueryResults metrics = result.metrics().queryMetrics(
                MetricsFilter.builder()
                        .addNameFilter((MetricNameFilter) Metrics.counter(RecommendationTransformer.ProductRecommendationModel.class, SAVINGS_RECOMMENDATION_COUNTER))
                        .build()
        );

        long savingsCounter = metrics.getCounters().iterator().next().getAttempted();
        assertEquals(1, savingsCounter);

        metrics = result.metrics().queryMetrics(
                MetricsFilter.builder()
                        .addNameFilter((MetricNameFilter) Metrics.counter(RecommendationTransformer.ProductRecommendationModel.class, LANDING_RECOMMENDATION_COUNTER))
                        .build()
        );

        long landingCounter = metrics.getCounters().iterator().next().getAttempted();
        assertEquals(0, landingCounter);

        metrics = result.metrics().queryMetrics(
                MetricsFilter.builder()
                        .addNameFilter((MetricNameFilter) Metrics.counter(RecommendationTransformer.ProductRecommendationModel.class, GENERIC_RECOMMENDATION_COUNTER))
                        .build()
        );

        long genericCounter = metrics.getCounters().iterator().next().getAttempted();
        assertEquals(1, genericCounter);
    }
}
