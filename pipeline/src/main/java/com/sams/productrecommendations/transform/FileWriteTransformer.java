package com.sams.productrecommendations.transform;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;
import org.joda.time.Duration;

import static com.sams.productrecommendations.util.ApplicationConstants.BUILD_FIXED_WINDOWS;
import static com.sams.productrecommendations.util.ApplicationConstants.CREATE_PARQUET_FILE;
import static com.sams.productrecommendations.util.ApplicationConstants.FEED_TYPE_COLD;
import static com.sams.productrecommendations.util.ApplicationConstants.GENERIC_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.MEMBERSHIP_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.PARQUET_FILE_EXTENSION;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_RECOMMENDATION;

public class FileWriteTransformer extends PTransform<PCollection<GenericRecord>, WriteFilesResult<Void>> {

    ValueProvider<String> module;

    ValueProvider<String> outPutFilePath;

    ValueProvider<String> feedType;

    public FileWriteTransformer(ValueProvider<String> module, ValueProvider<String> outPutFilePath) {
        this.module = module;
        this.outPutFilePath = outPutFilePath;
    }

    @Override
    public WriteFilesResult<Void> expand(PCollection<GenericRecord> input) {
        return input
                .apply(
                        BUILD_FIXED_WINDOWS,
                        Window.<GenericRecord>into(FixedWindows.of(Duration.standardMinutes(10)))
                                .withTimestampCombiner(TimestampCombiner.LATEST)
                                .triggering(AfterWatermark.pastEndOfWindow())
                                .withAllowedLateness(Duration.ZERO)
                                .accumulatingFiredPanes())
                .apply(
                        CREATE_PARQUET_FILE,
                        FileIO.<GenericRecord>write().via(
                                        ParquetIO.sink(getOutPutSchemaConverter(String.valueOf(module), String.valueOf(feedType)))
                                                .withCompressionCodec(CompressionCodecName.SNAPPY))
                                .to(outPutFilePath)
                                .withSuffix(PARQUET_FILE_EXTENSION));
    }

    public static Schema getOutPutSchemaConverter(String recommendationModel, String feedType) {
        if (recommendationModel.equalsIgnoreCase(SAVINGS_RECOMMENDATION) && feedType.contains(FEED_TYPE_COLD)) {
            return new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(SAVINGS_PARQUET_SCHEMA));
        } else if(recommendationModel.equalsIgnoreCase(SAVINGS_RECOMMENDATION) ){
            return new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(MEMBERSHIP_PARQUET_SCHEMA));
        }else {
            return new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(GENERIC_PARQUET_SCHEMA));
        }
    }
}
