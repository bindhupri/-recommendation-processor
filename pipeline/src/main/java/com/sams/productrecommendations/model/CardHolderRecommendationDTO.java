package com.sams.productrecommendations.model;

import java.io.Serializable;

public record CardHolderRecommendationDTO(String membershipUUID, String rankedProducts, String modelType)
        implements Serializable {
}
