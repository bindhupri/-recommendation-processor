package com.sams.productrecommendations.model;

import java.io.Serializable;

public record RyeRecommendationDTO(String membershipNumber, String productIds, String lastPurchaseDate,
                                   String lastPurchaseUnits, String model) implements Serializable {
}
