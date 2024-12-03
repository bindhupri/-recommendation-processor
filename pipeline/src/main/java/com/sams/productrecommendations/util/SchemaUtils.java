package com.sams.productrecommendations.util;

import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;

import lombok.experimental.UtilityClass;

import static com.sams.productrecommendations.util.ApplicationConstants.FEED_TYPE_COLD;
import static com.sams.productrecommendations.util.ApplicationConstants.GENERIC_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.LANDING_PAGE_COLD_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.MODULE_LANDING_PAGE;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_RECOMMENDATION;

@UtilityClass
public class SchemaUtils {

    public static Schema getSchemaByFeedType(String module, String feedType) {
        if(module!=null){
            if ( module.equalsIgnoreCase(SAVINGS_RECOMMENDATION)) {
                return new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(SAVINGS_PARQUET_SCHEMA));
            } else if( module.equalsIgnoreCase(MODULE_LANDING_PAGE) && feedType.contains(FEED_TYPE_COLD)){
                return new AvroSchemaConverter()
                        .convert(MessageTypeParser.parseMessageType(LANDING_PAGE_COLD_PARQUET_SCHEMA));
            }
        }
        return new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(GENERIC_PARQUET_SCHEMA));
    }
}
