package com.sams.productrecommendations.model;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

import java.io.Serializable;

@DefaultCoder(AvroCoder.class)
public record MembershipCardHolderDTO(String cardNumber, String membershipId, String membershipUUID, String modelType,
                                      String rankedProducts) implements Serializable {
}