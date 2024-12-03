package com.sams.productrecommendations.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sams.productrecommendations.model.CardHolderRecommendationDTO;
import com.sams.productrecommendations.model.CosmosRyeProductDTO;
import com.sams.productrecommendations.model.CosmosRyeProductMetaDataDTO;
import com.sams.productrecommendations.model.CosmosRyeRecommendationDTO;
import com.sams.productrecommendations.model.LandingColdMembershipRecoModel;
import com.sams.productrecommendations.model.MembershipCardHolderDTO;
import com.sams.productrecommendations.model.MembershipRecommendationDTO;
import com.sams.productrecommendations.model.RyeRecommendationDTO;
import com.sams.productrecommendations.util.ObjectMapperHelper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import static com.sams.productrecommendations.util.ApplicationConstants.CATEGORY;
import static com.sams.productrecommendations.util.ApplicationConstants.GENERIC_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.LANDING_PAGE_COLD_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.LAST_PURCHASE_DATE;
import static com.sams.productrecommendations.util.ApplicationConstants.LAST_PURCHASE_UNITS;
import static com.sams.productrecommendations.util.ApplicationConstants.MEMBERSHIP_NUMBER;
import static com.sams.productrecommendations.util.ApplicationConstants.MEMBERSHIP_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.MEMBERSHIP_UUID;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.RYE_PRODUCT_ID;
import static com.sams.productrecommendations.util.ApplicationConstants.SAVINGS_PARQUET_SCHEMA;
import static com.sams.productrecommendations.util.ApplicationConstants.SOURCE_ID;
import static com.sams.productrecommendations.util.ApplicationConstants.MODEL_TYPE;
import static com.sams.productrecommendations.util.ApplicationConstants.RANKED_PRODUCTS;
import static com.sams.productrecommendations.util.ApplicationConstants.WARM_START_OUTPUT_PARQUET_SCHEMA;

public class RecommendationJunitStubUtils {

    public static List<GenericRecord> savingsMembershipGenericRecord() {
        List<GenericRecord> genericRecordList = new ArrayList<>();
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(SAVINGS_PARQUET_SCHEMA));
        GenericRecord record = new GenericData.Record(schema);
        record.put(SOURCE_ID, Long.parseLong("123131313"));
        record.put(RANKED_PRODUCTS, "product1,product2,product3,product4");
        record.put(MODEL_TYPE, "savings");
        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(SOURCE_ID, Long.parseLong("1231131233"));
        record2.put(RANKED_PRODUCTS, "product1,product5,product6,product4");
        record2.put(MODEL_TYPE, "savings");
        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(SOURCE_ID, Long.parseLong("12321311"));
        record3.put(RANKED_PRODUCTS, "product7,product8,product9,product10");
        record3.put(MODEL_TYPE, "savings");
        genericRecordList.add(record);
        genericRecordList.add(record2);
        genericRecordList.add(record3);
        return genericRecordList;
    }

    public static List<GenericRecord> savingsMembershipGenericRecordString() {
        List<GenericRecord> genericRecordList = new ArrayList<>();
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(GENERIC_PARQUET_SCHEMA));
        GenericRecord record = new GenericData.Record(schema);
        record.put(SOURCE_ID,"123131313");
        record.put(RANKED_PRODUCTS, "product1,product2,product3,product4");
        record.put(MODEL_TYPE, "savings");
        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(SOURCE_ID, "1231131233");
        record2.put(RANKED_PRODUCTS, "product1,product5,product6,product4");
        record2.put(MODEL_TYPE, "savings");
        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(SOURCE_ID, "12321311");
        record3.put(RANKED_PRODUCTS, "product7,product8,product9,product10");
        record3.put(MODEL_TYPE, "savings");
        genericRecordList.add(record);
        genericRecordList.add(record2);
        genericRecordList.add(record3);
        return genericRecordList;
    }

    public static List<GenericRecord> landingColdMembershipGenericRecord() {
        List<GenericRecord> genericRecordList = new ArrayList<>();
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(LANDING_PAGE_COLD_PARQUET_SCHEMA));
        GenericRecord record = new GenericData.Record(schema);
        record.put(SOURCE_ID, ("123131313"));
        record.put(RANKED_PRODUCTS, "product1,product2,product3,product4");
        record.put(MODEL_TYPE, "sng_landing_page");
        record.put(CATEGORY,Long.parseLong("1"));
        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(SOURCE_ID, "1231131233");
        record2.put(RANKED_PRODUCTS, "product1,product5,product6,product4");
        record2.put(MODEL_TYPE, "sng_landing_page");
        record2.put(CATEGORY,Long.parseLong("2"));
        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(SOURCE_ID, "12321311");
        record3.put(RANKED_PRODUCTS, "product7,product8,product9,product10");
        record3.put(MODEL_TYPE, "sng_landing_page");
        record3.put(CATEGORY,Long.parseLong("3"));
        genericRecordList.add(record);
        genericRecordList.add(record2);
        genericRecordList.add(record3);
        return genericRecordList;
    }


    public static List<GenericRecord> ryeGenericRecordList(){
        List<GenericRecord> genericRecordList = new ArrayList<>();
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(RYE_PARQUET_SCHEMA));
        GenericRecord record = new GenericData.Record(schema);
        record.put(MEMBERSHIP_NUMBER, Long.parseLong("1231313131"));
        record.put(RYE_PRODUCT_ID,"product1");
        record.put(LAST_PURCHASE_DATE, "2020-01-01");
        record.put(LAST_PURCHASE_UNITS, "10");
        record.put(MODEL_TYPE, "rye");

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(MEMBERSHIP_NUMBER, Long.parseLong("1231313132"));
        record2.put(RYE_PRODUCT_ID,"product2");
        record2.put(LAST_PURCHASE_DATE, "2020-01-02");
        record2.put(LAST_PURCHASE_UNITS, "10");
        record2.put(MODEL_TYPE, "rye");

        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(MEMBERSHIP_NUMBER, Long.parseLong("1231313133"));
        record3.put(LAST_PURCHASE_DATE, "2020-01-03");
        record3.put(RYE_PRODUCT_ID,"product3");
        record3.put(LAST_PURCHASE_UNITS, "10");
        record3.put(MODEL_TYPE, "rye");

        genericRecordList.add(record);
        genericRecordList.add(record2);
        genericRecordList.add(record3);
        return genericRecordList;
    }


    public static List<GenericRecord> savingsMembershipGenericRecordWithUUID() {
        List<GenericRecord> genericRecordList = new ArrayList<>();
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(WARM_START_OUTPUT_PARQUET_SCHEMA));

        GenericRecord record = new GenericData.Record(schema);
        record.put(MEMBERSHIP_UUID, "1231313asdfsadf13");
        record.put(RANKED_PRODUCTS, "product1,product2,product3,product4");
        record.put(MODEL_TYPE, "savings");
        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(MEMBERSHIP_UUID, "asdfsdf1231313");
        record2.put(RANKED_PRODUCTS, "product1,product5,product6,product4");
        record2.put(MODEL_TYPE, "savings");
        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(MEMBERSHIP_UUID, "asdfsdfasdf123131231313");
        record3.put(RANKED_PRODUCTS, "product7,product8,product9,product10");
        record3.put(MODEL_TYPE, "savings");
        genericRecordList.add(record);
        genericRecordList.add(record2);
        genericRecordList.add(record3);
        return genericRecordList;
    }

    public static List<GenericRecord> persistMembershipGenericRecord() {
        List<GenericRecord> genericRecordList = new ArrayList<>();
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(GENERIC_PARQUET_SCHEMA));

        GenericRecord record = new GenericData.Record(schema);
        record.put(SOURCE_ID, "123131313");
        record.put(RANKED_PRODUCTS, "product1,product2,product3,product4");
        record.put(MODEL_TYPE, "persist");
        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(SOURCE_ID, "1231131233");
        record2.put(RANKED_PRODUCTS, "product1,product5,product6,product4");
        record2.put(MODEL_TYPE, "persist");
        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(SOURCE_ID, "123213113");
        record3.put(RANKED_PRODUCTS, "product7,product8,product9,product10");
        record3.put(MODEL_TYPE, "persist");
        genericRecordList.add(record);
        genericRecordList.add(record2);
        genericRecordList.add(record3);
        return genericRecordList;
    }

    public static List<GenericRecord> complementaryMembershipGenericRecord() {
        List<GenericRecord> genericRecordList = new ArrayList<>();
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(GENERIC_PARQUET_SCHEMA));

        GenericRecord record = new GenericData.Record(schema);
        record.put(SOURCE_ID, "123131313");
        record.put(RANKED_PRODUCTS, "product1,product2,product3,product4");
        record.put(MODEL_TYPE, "complementary");
        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(SOURCE_ID, "1231131233");
        record2.put(RANKED_PRODUCTS, "product1,product5,product6,product4");
        record2.put(MODEL_TYPE, "complementary");
        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(SOURCE_ID, "123213113");
        record3.put(RANKED_PRODUCTS, "product7,product8,product9,product10");
        record3.put(MODEL_TYPE, "complementary");
        genericRecordList.add(record);
        genericRecordList.add(record2);
        genericRecordList.add(record3);
        return genericRecordList;
    }


    public static List<GenericRecord> membershipGenericRecord() {
        List<GenericRecord> genericRecordList = new ArrayList<>();
        Schema schema = new AvroSchemaConverter().convert(MessageTypeParser.parseMessageType(MEMBERSHIP_PARQUET_SCHEMA));

        GenericRecord record = new GenericData.Record(schema);
        record.put(MEMBERSHIP_UUID, "1231313asdfsadf13");
        record.put(RANKED_PRODUCTS, "product1,product2,product3,product4");
        record.put(MODEL_TYPE, "savings");
        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(MEMBERSHIP_UUID, "asdfsdf1231313");
        record2.put(RANKED_PRODUCTS, "product1,product5,product6,product4");
        record2.put(MODEL_TYPE, "savings");
        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(MEMBERSHIP_UUID, "asdfsdfasdf123131231313");
        record3.put(RANKED_PRODUCTS, "product7,product8,product9,product10");
        record3.put(MODEL_TYPE, "savings");
        genericRecordList.add(record);
        genericRecordList.add(record2);
        genericRecordList.add(record3);
        return genericRecordList;
    }



    public static List<CardHolderRecommendationDTO> membershipUUIDRecommendationList() {
        List<CardHolderRecommendationDTO> recommendationList = new ArrayList<>();
        CardHolderRecommendationDTO recommendation = new CardHolderRecommendationDTO("1231313asdfsadf13", "product1,product2,product3,product4", "savings");
        CardHolderRecommendationDTO recommendation2 = new CardHolderRecommendationDTO("asdfsdf1231313", "product1,product5,product6,product4", "savings");
        CardHolderRecommendationDTO recommendation3 = new CardHolderRecommendationDTO("asdfsdfasdf123131231313", "product7,product8,product9,product10", "savings");

        recommendationList.add(recommendation);
        recommendationList.add(recommendation2);
        recommendationList.add(recommendation3);

        return recommendationList;
    }

    public static List<MembershipRecommendationDTO> genericProductRecommendationModel(String module) {
        List<MembershipRecommendationDTO> recommendationList = new ArrayList<>();
        MembershipRecommendationDTO recommendation = new MembershipRecommendationDTO("123131313", "product1,product2,product3,product4", module);
        MembershipRecommendationDTO recommendation2 = new MembershipRecommendationDTO("1231131233", "product1,product5,product6,product4", module);
        MembershipRecommendationDTO recommendation3 = new MembershipRecommendationDTO("12321311", "product7,product8,product9,product10", module);

        recommendationList.add(recommendation);
        recommendationList.add(recommendation2);
        recommendationList.add(recommendation3);

        return recommendationList;
    }

    public static List<CardHolderRecommendationDTO> cardHolderProductRecommendationModel2(String module) {
        List<CardHolderRecommendationDTO> recommendationList = new ArrayList<>();
        CardHolderRecommendationDTO recommendation = new CardHolderRecommendationDTO("123131313", "product1,product2,product3,product4", module);
        CardHolderRecommendationDTO recommendation2 = new CardHolderRecommendationDTO("1231131233", "product1,product5,product6,product4", module);
        CardHolderRecommendationDTO recommendation3 = new CardHolderRecommendationDTO("12321311", "product7,product8,product9,product10", module);

        recommendationList.add(recommendation);
        recommendationList.add(recommendation2);
        recommendationList.add(recommendation3);

        return recommendationList;
    }

    public static List<MembershipCardHolderDTO> genericCardRecommendationModel(String module) {
        List<MembershipCardHolderDTO> recommendationList = new ArrayList<>();
        MembershipCardHolderDTO recommendation = new MembershipCardHolderDTO("test","test","123131313", "product1,product2,product3,product4", module);
        MembershipCardHolderDTO recommendation2 = new MembershipCardHolderDTO("test","test","1231131233", "product1,product5,product6,product4", module);
        MembershipCardHolderDTO recommendation3 = new MembershipCardHolderDTO("test","test","12321311", "product7,product8,product9,product10", module);

        recommendationList.add(recommendation);
        recommendationList.add(recommendation2);
        recommendationList.add(recommendation3);

        return recommendationList;
    }

    public static List<CardHolderRecommendationDTO> membershipUUIDRecommendationList2() {
        List<CardHolderRecommendationDTO> recommendationList = new ArrayList<>();
        CardHolderRecommendationDTO recommendation = new CardHolderRecommendationDTO("123131313", "product1,product2,product3,product4", "savings");
        CardHolderRecommendationDTO recommendation2 = new CardHolderRecommendationDTO("1231131233", "product1,product5,product6,product4", "savings");
        CardHolderRecommendationDTO recommendation3 = new CardHolderRecommendationDTO("12321311", "product7,product8,product9,product10", "savings");

        recommendationList.add(recommendation);
        recommendationList.add(recommendation2);
        recommendationList.add(recommendation3);

        return recommendationList;
    }

    public static List<LandingColdMembershipRecoModel> genericLandingColdProductRecommendationModel(String module) {
        List<LandingColdMembershipRecoModel> recommendationList = new ArrayList<>();
        LandingColdMembershipRecoModel recommendation = new LandingColdMembershipRecoModel("123131313", "product1,product2,product3,product4", module,"1");
        LandingColdMembershipRecoModel recommendation2 = new LandingColdMembershipRecoModel("1231131233", "product1,product5,product6,product4", module,"2");
        LandingColdMembershipRecoModel recommendation3 = new LandingColdMembershipRecoModel("12321311", "product7,product8,product9,product10", module,"3");

        recommendationList.add(recommendation);
        recommendationList.add(recommendation2);
        recommendationList.add(recommendation3);

        return recommendationList;
    }

    public static List<RyeRecommendationDTO> ryeRecords() {
        List<RyeRecommendationDTO>  ryeRecommendationDTOList = new ArrayList<>();
        RyeRecommendationDTO ryeRecommendationDTO = new RyeRecommendationDTO("1231313131", "product1", "2020-01-01", "10", "rye");
        RyeRecommendationDTO ryeRecommendationDTO2 = new RyeRecommendationDTO("1231313132", "product2", "2020-01-02", "10", "rye");
        RyeRecommendationDTO ryeRecommendationDTO3 = new RyeRecommendationDTO("1231313133", "product3", "2020-01-03", "10", "rye");
        ryeRecommendationDTOList.add(ryeRecommendationDTO);
        ryeRecommendationDTOList.add(ryeRecommendationDTO2);
        ryeRecommendationDTOList.add(ryeRecommendationDTO3);
        return ryeRecommendationDTOList;
    }

    public static List<String> ryeCosmosToJson() throws JsonProcessingException {
        CosmosRyeRecommendationDTO dto = new CosmosRyeRecommendationDTO("1231313131", "1231313131", "rye", "10", "rye", List.of(new CosmosRyeProductDTO("productId1", new CosmosRyeProductMetaDataDTO("2020-01-01", "10"))),10);
        ObjectMapper mapper = ObjectMapperHelper.get();
        return List.of(mapper.writeValueAsString(dto));

    }

    public static List<CosmosRyeRecommendationDTO> ryeCosmosList() throws JsonProcessingException {
        CosmosRyeRecommendationDTO dto = new CosmosRyeRecommendationDTO("1231313131", "1231313131", "rye", "10", "rye", List.of(new CosmosRyeProductDTO("productId1", new CosmosRyeProductMetaDataDTO("2020-01-01", "10"))),10);
        return List.of(dto);

    }


}
