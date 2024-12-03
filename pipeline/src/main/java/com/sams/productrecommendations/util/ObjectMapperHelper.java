package com.sams.productrecommendations.util;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectMapperHelper {

    private static ObjectMapper mapper;

    private ObjectMapperHelper() {
    }

    public static ObjectMapper get() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }
}
