package com.sams.productrecommendations.model;



import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;

@JsonSerialize
public record CosmosRyeProductDTO(String productId, CosmosRyeProductMetaDataDTO metaData) implements Serializable {
}
