package com.sams.productrecommendations.util;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

/*
 An Utility class to get secrets from GCP Secret Manger
*/
@Slf4j
@SuppressWarnings("java:S112")
public class SecretManagerUtil {

    private SecretManagerUtil() {
    }

    public static String getSecret(String projectId, String secretName) {
        try (SecretManagerServiceClient secretManagerServiceClient =
                     SecretManagerServiceClient.create()) {
            SecretVersionName name = SecretVersionName.of(projectId, secretName, "latest");
            AccessSecretVersionResponse accessSecretVersionResponse =
                    secretManagerServiceClient.accessSecretVersion(name);
            return accessSecretVersionResponse.getPayload().getData().toStringUtf8();
        } catch (IOException ex) {
            log.error(
                    "Error while fetching Secret for projectId:{}, secreName:{}, error:",
                    projectId,
                    secretName,
                    ex);
            throw new RuntimeException(ex);
        }
    }
}
