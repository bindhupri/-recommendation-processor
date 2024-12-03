package com.sams.productrecommendations.config;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface BatchPipelineOptions extends GcpOptions {
    /***
     * All pipeline specific options are passed through execution commands
     */

    @Description("OutPutFile Path")
    @Validation.Required
    ValueProvider<String> getOutPutFilePath();

    void setOutPutFilePath(ValueProvider<String> outPutFilePath);

    @Description("Path of the file to read from")
    @Validation.Required
    ValueProvider<String> getInputFile();

    void setInputFile(ValueProvider<String> inputFile);

    @Description("Recommendation Module")
    @Validation.Required
    ValueProvider<String> getModule();

    void setModule(ValueProvider<String> module);

    @Description("Feed Type")
    @Validation.Required
    ValueProvider<String> getFeedType();

    void setFeedType(ValueProvider<String> feedType);

    @Description("WriteToCosmos")
    @Validation.Required
    ValueProvider<String> getWriteToCosmos();

    void setWriteToCosmos(ValueProvider<String> writeToCosmos);


    @Description("WriteToCloudStorage")
    @Validation.Required
    ValueProvider<String> getWriteToCloudStorage();

    void setWriteToCloudStorage(ValueProvider<String> writeToCloudStorage);

    @Description("WriteToBigQuery")
    @Validation.Required
    ValueProvider<String> getWriteToBigQuery();

    void setWriteToBigQuery(ValueProvider<String> writeToBigQuery);


    @Description("uuidToAudit")
    ValueProvider<String> getUuidToAudit();

    void setUuidToAudit(ValueProvider<String> uuidToAudit);

    @Description("cardHolderRecommendation")
    ValueProvider<String> getCardHolderRecommendation();

    void setCardHolderRecommendation(ValueProvider<String> cardHolderRecommendation);

    @Description("cosmosBatchSize")
    ValueProvider<String> getCosmosBatchSize();
    void setCosmosBatchSize(ValueProvider<String> cosmosBatchSize);
}
