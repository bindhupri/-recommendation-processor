package com.sams.productrecommendations.model;

import java.io.Serializable;

public record LandingColdMembershipRecoModel(String id, String recommendations,
                                             String modelType, String category) implements Serializable{
}
