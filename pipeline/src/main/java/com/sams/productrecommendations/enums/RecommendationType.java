package com.sams.productrecommendations.enums;

import java.util.Arrays;

import lombok.Getter;

public enum RecommendationType {
    SAVINGS("savings", "personalized_savings_products", 3 * 60 * 60 * 24),
    RYE("rye", "frequently_bought_products", 3 * 60 * 60 * 24),
    LANDING_PAGE("landing_page", "personalized_home_page_products", 3 * 60 * 60 * 24),
    PERSIST("persist", "substitution_products", 20 * 60 * 60 * 24),
    COMPLIMENTARY("complementary", "complementary_products", 10 * 60 * 60 * 24),
    DEFAULT("default", "default_recommendations", 10 * 60 * 60 * 24);


    @Getter
    private final String value;
    @Getter
    private final String strategyName;
    @Getter
    private final Integer ttlInSecs;

    RecommendationType(String value, String strategyName, Integer ttlInSecs) {
        this.value = value;
        this.strategyName = strategyName;
        this.ttlInSecs = ttlInSecs;
    }

    public static RecommendationType fromString(String value) {
        return Arrays.stream(RecommendationType.values())
                .filter(recommendationType -> recommendationType.getValue().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid RecommendationType:" + value));
    }
}
