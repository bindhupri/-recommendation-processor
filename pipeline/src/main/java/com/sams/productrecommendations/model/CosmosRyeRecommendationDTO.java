package com.sams.productrecommendations.model;



import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.List;

import lombok.Builder;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonSerialize
public record CosmosRyeRecommendationDTO(String id,
                                         String recommendationId,
                                         String recommendationIdType,
                                         String strategyName,
                                         String modelType,
                                         List<CosmosRyeProductDTO> recommendedProductsV1,
                                         Integer ttl) implements Serializable {
}
