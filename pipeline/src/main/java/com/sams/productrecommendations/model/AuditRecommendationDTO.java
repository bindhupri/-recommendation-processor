package com.sams.productrecommendations.model;

import java.io.Serializable;

public record AuditRecommendationDTO(String uuid, String recommendationType, String feedType, String status,
                                     String processedTime, Long count) implements Serializable {
}
