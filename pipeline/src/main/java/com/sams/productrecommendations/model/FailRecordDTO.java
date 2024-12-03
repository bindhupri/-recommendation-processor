package com.sams.productrecommendations.model;

import java.io.Serializable;

public record FailRecordDTO(
        String errorMessage, String errorStep, Object object, Throwable throwable)
        implements Serializable {
}
