package com.sams.productrecommendations.util;

public class ApplicationConstants {

    public static final String SOURCE_ID = "Id";
    public static final String RANKED_PRODUCTS = "reco";
    public static final String MEMBERSHIP_ID = "member_id";
    public static final String TOP_PRODUCTS = "topk";
    public static final String MODEL_TYPE = "model";
    public static final String MODEL_TYPE_NAME = "modelType";
    public static final String CATEGORY = "category";
    public static final String CLUB_ID = "club_id";
    public static final String CATEGORY_ID = "category_id";
    public static final String POPULAR_PRODUCTS = "popular_products";

    public static final String RYE_PRODUCT_ID = "prod_id";
    public static final String LAST_PURCHASE_DATE = "last_purchase_date";
    public static final String LAST_PURCHASE_UNITS = "last_purchase_units";
    public static final String RECOMMENDED_PRODUCT_SUBSTITUTIONS = "prod_id_sub_list";
    public static final String RECOMMENDED_COMPLEMENTARY_PRODUCTS = "ranked_products";

    public static final String MEMBERSHIP_NUMBER = "membership_nbr";

    public static final String BIGQUERY_CARD_NUMBER = "cardNumber";
    public static final String BIGQUERY_MEMBERSHIP_ID = "membershipId";
    public static final String BIGQUERY_MODEL_TYPE = "modelType";
    public static final String BIGQUERY_MEMBERSHIP_UUID = "membershipUUID";
    public static final String BIGQUERY_RANKED_PRODUCTS = "rankedProducts";

    public static final String BIGQUERY_DATA_TYPE_STRING = "STRING";
    public static final String BIGQUERY_DATA_TYPE_NUMERIC = "NUMERIC";

    public static final String BIGQUERY_REQUIRED = "REQUIRED";
    public static final String BIGQUERY_NULLABLE = "NULLABLE";

    public static final String JSON_ATTRIBUTES = "attributes";
    public static final String JSON_MESSAGE_ID = "messageId";
    public static final String JSON_PUBLISH_TIME = "publishTime";
    public static final String JSON_ORDERING_KEY = "orderingKey";
    public static final String JSON_DATA = "data";
    public static final String MEMBERSHIP_UUID = "UUID";

    public static final String GENERIC_AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "recommendation",
              "fields": [
                {
                  "name": "Id",
                  "type": "string"
                },
                {
                  "name": "reco",
                  "type": "string"
                },
                {
                  "name": "model",
                  "type": "string"
                }
              ]
            }
            """;

    public static final String SAVINGS_COLD_AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "recommendation",
              "fields": [
                {
                  "name": "Id",
                  "type": "int"
                },
                {
                  "name": "reco",
                  "type": "string"
                },
                {
                  "name": "model",
                  "type": "string"
                }
              ]
            }
            """;
    public static final String LANDING_COLD_AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "recommendation",
              "fields": [
                {
                  "name": "Id",
                  "type": "int"
                },
                {
                  "name": "reco",
                  "type": "string"
                },
                {
                  "name": "model",
                  "type": "string"
                },
                {
                  "name": "category",
                  "type": "int"
                }
              ]
            }
            """;

    public static final String MEMBERSHIPUUID_AVRO_SCHEMA = """
            {
              "type": "record",
              "name": "membershipRecommendation",
              "fields": [
                {
                  "name": "UUID",
                  "type": "string"
                },
                {
                  "name": "reco",
                  "type": "string"
                },
                {
                  "name": "model",
                  "type": "string"
                }
              ]
            }
            """;

    public static final String GENERIC_PARQUET_SCHEMA = """
            message offers { required binary Id (UTF8); required binary reco (UTF8);required binary model (UTF8);}
            """;

    public static final String MEMBERSHIP_PARQUET_SCHEMA = """
            message offers { required binary UUID(UTF8); required binary reco (UTF8);required binary model (UTF8);}
            """;
    public static final String SAVINGS_PARQUET_SCHEMA = """
            message offers { required int64 Id; required binary reco (UTF8);required binary model (UTF8);}
            """;

    public static final String WARM_START_OUTPUT_PARQUET_SCHEMA = """
            message offers { required binary UUID (UTF8); required binary reco (UTF8);required binary model (UTF8);}
            """;

    public static final String PERSIST_PARQUET_SCHEMA =
            "message offers { required binary prod_id (UTF8); required binary prod_id_sub_list (UTF8);required"
                    + " binary model (UTF8);}";

    public static final String COMPLEMENTARY_PARQUET_SCHEMA =
            "message offers { required binary prod_id (UTF8); required binary ranked_products (UTF8);required"
                    + " binary model (UTF8);}";

    public static final String LANDING_PAGE_COLD_PARQUET_SCHEMA = """
            message schema {required binary Id (UTF8);required binary reco (UTF8); required binary model (UTF8);required int64 category;}
            """;

    public static final String LANDING_PAGE_WARM_PARQUET_SCHEMA =
            "message schema { " +
                    "required int64 member_id; " +
                    "required binary topk (STRING);" +
                    "required binary model (STRING);" +
                    "}";

    public static final String RYE_PARQUET_SCHEMA = """
            message offers {required int64 membership_nbr; required binary prod_id (UTF8); optional binary last_purchase_date (UTF8); optional binary last_purchase_units (UTF8);required binary model (UTF8);}
            """;
    public static final String MODULE_SAVINGS = "savings";
    public static final String FEED_TYPE_COLD = "cold";
    public static final String FEED_TYPE_WARM = "warm";
    public static final String MODULE_RYE = "rye";
    public static final String MODULE_PERSIST = "persist";
    public static final String MODULE_COMPLIMENTARY = "complementary";
    public static final String MODULE_LANDING_PAGE = "sng_landing_page";

    public static final String MATCH = "Match File Location";
    public static final String READ_FILE_MATCHES = "Read All File Matches";
    public static final String READ_PARQUET_FILE = "Read Parquet File";
    public static final String BUILD_RECOMM_MODEL = "RecommendationModel";
    public static final String ID_UUID_MAPPING = "Map MembershipID to UUID";
    public static final String MEM_UUID_GENERIC = "MembershipUUIDToGenericRecord";
    public static final String BUILD_FIXED_WINDOWS = " Creating Windowing For Write Generic Record";
    public static final String CREATE_PARQUET_FILE = "Create A New Parquet File";
    public static final String OUTPUT_FILE_LOCATION = "OutPut File Location";
    public static final String PUBLISH_OUT_FILE_LOC = "Send New File Location To Topic";
    public static final String READ_PUB_SUB_MESSAGE = "Read Pub Sub Message";
    public static final String PARSE_PAYLOAD = "Parse Payload";
    public static final String SAVINGS_COLD_RECOMMENDATION = "savingsCold";
    public static final String SAVINGS_WARM_RECOMMENDATION = "savingsWarm";
    public static final String LANDING_PAGE_COLD_RECOMMENDATION = "landingPageCold";
    public static final String LANDING_PAGE_WARM_RECOMMENDATION = "landingPageWarm";
    public static final String BIGQUERY_MAPPING_STATEFUL = "BigQueryMappingStateful";
    public static final String GROUPING = "Grouping";
    public static final String PARQUET_FILE_EXTENSION = ".parquet";

    public static final String GET_UUID_FOR_MEMBERSHIP = "Get Membership UUID";

    public static final String MAP_BIGQUERY_MEMBERSHIP_RECOMM_MODEL = "Mapping Bigquery Response To Recommendation Model";

    public static final String BUILD_COSMOS_DOCUMENT = "Build Comsos Document";

    public static final String INGEST_TO_COSMOS = "Ingest To Cosmos";

    public static final String BUILD_PRODUCT_RECOMMENDATIONS_KVPAIR = "Build Product Recommendations KV Pair";

    public static final String TRANSFORM_KV_TO_LIST = "Transorm KV to List";

    public static final String COSMOS_STATEFUL_PROCESSING = "Stateful Processing";

    public static final String COSMOS_DOC_GENERIC_RECORD = "Cosmos Document to Generic Record";
    public static final String COSMOS_FILE_WRITE_WINDOWING = "Creating Windowing to Write Generic Record";

    public static final String COSMOS_FAILURE_NEW_PARQUET = "Create A New Parquet File For Cosmos Failures";

    public static final String HOST_SECRET_NAME = "recommendation_db_host";
    public static final String ACCOUNY_KEY_SECRET_NAME = "recommendation_db_secret";
    public static final String COSMOS_DB_NAME = "recommendations";
    public static final String COSMOS_CONTAINER_NAME = "product-recommendations";
    public static final String PRODUCT_RECOMMENDATIONS_PARTITIONID = "recommendationId";
    public static final Integer COSMOS_IO_BATCH_SIZE = 100;
    public static final String COSMOS_DOCUMENT_PARQUET_SCHEMA = """
            message recommendationDocument { required binary id (UTF8); required binary recommendationId (UTF8);required binary recommendationIdType (UTF8); required binary strategyName (UTF8); optional binary modelType (UTF8); required binary recommendedProducts (UTF8); optional binary recommendedUPCs (UTF8); optional binary recommendedItems (UTF8); optional int32 ttl; }
            """;
    public static final String COMSOS_DOCUMENT_AVRO_SCHEMA =
            """
                    {
                        "type" : "record",
                        "name" : "recommendationDocument",
                        "fields": [
                            {
                              "name": "id",
                              "type": "string"
                            },
                            {
                              "name": "recommendationId",
                              "type": "string"
                            },
                            {
                              "name": "recommendationIdType",
                              "type": "string"
                            },
                            {
                              "name": "strategyName",
                              "type": "string"
                            },
                            {
                              "name": "modelType",
                              "type": "string"
                            },
                            {
                              "name": "recommendedProducts",
                              "type": "string"
                            },
                            {
                              "name": "recommendedUPCs",
                              "type": "string",
                              "default": null
                            },
                            {
                              "name": "recommendedItems",
                              "type": "string",
                              "default": null
                            },
                            {
                                "name": "ttl",
                                "type": "int",
                                "default": 0
                            }
                        ]
                    }
                    """;
    public static final String SAVINGS_RECOMMENDATION = "savings";
    public static final String SAVINGS_RECOMMENDATION_COLD = "savingsCold";

    public static final String SAVINGS_RECOMMENDATION_WRITE = "savingsWrite";
    public static final String SAVINGS_RECOMMENDATION_READ = "savingsRead";
    public static final String PERSIST_RECOMMENDATION = "persist";
    public static final String COMPLEMENTARY_RECOMMENDATION = "complementary";
    public static final String COSMOS_INGESTION_FAILURE = "cosmosIngestionFailure";
    public static final String RYE_RECOMMENDATION = "rye";
    public static final String RYE_COSMOS_FAILURE = "rye_cosmos_failure";
    public static final String LANDING_COSMOS_FAILURE = "landing_cosmos_failure";
    public static final String AUDIT = "audit";

    public static final String FILE_PATH = "file_path";
    public static final String MODULE = "module";
    public static final String FEED_TYPE = "feed_type";

    public static final String ID = "id";
    public static final String RECOMMENDATION_ID = "recommendationId";
    public static final String RECOMMENDATION_ID_TYPE = "recommendationIdType";
    public static final String STRATEGY_NAME = "strategyName";
    public static final String PRODUCT_ID = "productId";
    public static final String COSMOS_MODEL_TYPE = "modelType";
    public static final String RECOMMENDED_PRODUCTS = "recommendedProducts";
    public static final String RECOMMENDED_UPCS = "recommendedUPCs";
    public static final String RECOMMENDED_ITEMS = "recommendedItems";
    public static final String TTL = "ttl";
    public static final String COSMOS_LAST_PURCHASE_DATE = "lastPurchaseDate";
    public static final String COSMOS_LAST_PURCHASE_UNITS = "lastPurchaseUnits";
    public static final String COSMOS_FAILURES_OUTPUT_PATH = "cosmos_failures";
    public static final String RYE_COSMOS_FAILURES_OUTPUT_PATH = "rye_cosmos_failures";
    public static final int MAX_RETRY_ATTEMPTS_ON_THROTTLED_REQUESTS = 5;
    public static final int MAX_RETRY_WAIT_TIME = 30;
    public static final int DIRECT_CONFIG_CONNECT_TIMEOUT = 1;
    public static final String MEMBERSHIP_UUID_COUNTER = "membershipUUIDCounter";
    public static final String READ_CHANGE_FEED_FAILURE_FILES = "Read Change Feed Failures";
    public static final String READ_COSMOS_INGESTION_FAILURE_FILES = "Read Cosmos Ingestion Failures";
    public static final String TRANSFORM_FAILURE_PRODUCT_MODEL = "Transform Json To Model";
    public static final String TRANSFORM_PRODUCT_RECOMMENDATIONS = "Transform To Product Recommendations";
    public static final String PROCESS_RECO_COUNTER = "processFailureRecoCounter";

    public static final String BIGQUERY_COLUMN_RECO_UUID = "UUID";
    public static final String BIGQUERY_COLUMN_RECO_TYPE = "recommendation_type";
    public static final String BIGQUERY_COLUMN_RECORD_COUNT = "no_of_records";
    public static final String BIGQUERY_COLUMN_FEED_TYPE = "feed_type";
    public static final String BIGQUERY_COLUMN_STATUS = "status";
    public static final String BIGQUERY_COLUMN_PROCESSED_TIME = "processed_time";
    public static final String COUNT_RECORDS = "Count the number of records";
    public static final String AUDIT_STATUS_FILE_READ = "Parquet read count";
    public static final String AUDIT_STATUS_MODEL_CONSTRUCTION = "Compliant records converted to Model";

    public static final String AUDIT_BIGQUERY_WRITE = "Write Audit";
    public static final String BUILD_AUDIT_MODEL = "Build Audit Model";
    public static final String AUDIT_STATUS_DB_WRITE_FAILURE = "Number of records failed to insert to DB";
    public static final String AUDIT_STATUS_BIGQUERY_READ = "Number of records after BigQuery Join";
    public static final String AUDIT_STATUS_COLD_PARQUET_WRITE = "Number of cold start records to be written as parquet output";
    public static final String AUDIT_STATUS_COLD_PARQUET_READ = "Number of cold start records read from parquet input";
    public static final String AUDIT_STATUS_PARQUET_READ = "Number of records read from parquet input";
    public static final String AUDIT_STATUS_COLD_DB_WRITE = "Number of cold start records to be written to DB";
    public static final String AUDIT_STATUS_WARM_DB_WRITE = "Number of warm start records to be written to DB";
    public static final String AUDIT_STATUS_DB_WRITE = "Number of records to be written to DB";

    public static final String MEGHACACHE_ROLE = "meghacache.role";
    public static final String MEGHACACHE_CONFIG_NAME = "meghacache.config.name";
    public static final String CCM_RUNTIME_CONTEXT_ENVIRONMENTTYPE = "runtime.context.environmentType";
    public static final String CCM_RUNTIME_CONTEXT_APPNAME = "runtime.context.appName";
    public static final String CCM_IO_STRATI_RUNTIMECONTEXT = "io.strati.RuntimeContext";
    public static final String RETRY_COUNT_KEY = "retryCount";
    public static final int RETRY_COUNT_VALUE = 3;
    public static final String CHANGE_FEED_PENDING_FOLDER = "pending";
    public static final String CHANGE_FEED_FAILED_FOLDER = "failed";
    public static final String CHANGE_FEED_PROCESSED_FOLDER = "processed";
    public static final String FILTER_BY_BIG_QUERY_WRITE = "Filter By Big Query Write";
    public static final String BIG_QUERY_WRITE_TRANSFORM = "Big Query Write Transform";
    public static final String FILTER_BY_CLOUD_STORAGE_WRITE = "Filter By Cloud Storage Write";
    public static final String FILTER_BY_COSMOS_WRITE = "Filter By Cosmos Write";
    public static final String RECO_TO_GENERIC = "Recommendation Model To Generic";
    public static final String WRITE_TO_CLOUD_STORAGE = "Write To Cloud Storage";
    public static final String RECOMMENDATION_TYPE_MEMBERSHIP_NBR = "MembershipNbr";
    public static final String OPTIONS_FLAG_Y = "Y";
    public static final String RYE_RECOMMENDATION_COUNTER = "ryeRecommendationCounter";
    public static final String SAVINGS_RECOMMENDATION_COUNTER = "savingsRecommendationCounter";
    public static final String LANDING_RECOMMENDATION_COUNTER = "landingPageRecommendationCounter";
    public static final String PERSIST_RECOMMENDATION_COUNTER = "persistRecommendationCounter";
    public static final String GENERIC_RECOMMENDATION_COUNTER = "genericRecommendationCounter";
    public static final String COMPLEMENTARY_RECOMMENDATION_COUNTER = "complementaryRecommendationCounter";
    public static final String RYE_COSMOS_FAILURE_COUNTER = "ryeCosmosFailureCounter";
    public static final String RYE_COSMOS_RETRY_COUNTER = "ryeCosmosRetryCounter";
    public static final String COSMOS_RECOMMENDATIONS_DATABASE = "recommendations";
    public static final String COSMOS_RYE_DATABASE = "rye";
    public static final String COSMOS_RYE_CONTAINER = "rye-recommendations";
    public static final String COSMOS_FAILURE_MODEL_TRANSFORM = "Cosmos Failure Model Transform";
    public static final String COSMOS_FAILURE_READ_COUNT = "Cosmos Failure Read Count";
    public static final String AUDIT_COSMOS_FAILURE_READ = "Audit Cosmos Failure Read Count";
    public static final String WRITE_COSMOS_FAILURES_TO_GCS = "Write Cosmos Failures To GCS";
    public static final String RYE_COSMOS_FAILURE_MODEL_TRANSFORM = "Rye Cosmos Failure Model Transform";
    private ApplicationConstants() {

    }
}
