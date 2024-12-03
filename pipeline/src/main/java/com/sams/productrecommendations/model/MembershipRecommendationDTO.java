package com.sams.productrecommendations.model;

import java.io.Serializable;

public record MembershipRecommendationDTO(String id, String recommendations,
                                          String modelType) implements Serializable {
}