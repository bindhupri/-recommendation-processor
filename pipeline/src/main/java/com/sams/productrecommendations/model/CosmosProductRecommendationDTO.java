package com.sams.productrecommendations.model;

import java.io.Serializable;
import java.util.List;

import lombok.Builder;

@Builder
public record CosmosProductRecommendationDTO(
        String id,
        String recommendationId,
        String recommendationIdType,
        String strategyName,
        String modelType,
        List<String> recommendedProducts,
        List<String> recommendedUPCs,
        List<String> recommendedItems,
        Long categoryId,
        Integer ttl)
        implements Serializable {
}
