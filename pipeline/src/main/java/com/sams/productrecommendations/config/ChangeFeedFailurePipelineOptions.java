package com.sams.productrecommendations.config;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface ChangeFeedFailurePipelineOptions extends BatchPipelineOptions {

    @Description("ccm environment")
    @Validation.Required
    String getCcmEnvironment();

    void setCcmEnvironment(String ccmEnvironment);

    @Description("ccm appName")
    @Validation.Required
    String getCcmAppName();

    void setCcmAppName(String ccmAppName);

    @Description("ccm runtimeContext")
    @Validation.Required
    String getCcmRunTimeContext();

    void setCcmRunTimeContext(String ccmRunTimeContext);

    @Description("Meghacache roleName")
    @Validation.Required
    String getMeghaCacheRole();

    void setMeghaCacheRole(String meghaCacheRole);

    @Description("Meghacache configName")
    @Validation.Required
    String getMeghaCacheConfigName();

    void setMeghaCacheConfigName(String meghaCacheConfigName);

}
