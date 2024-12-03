package com.sams.productrecommendations.util;

public class QueryConstants {
    private QueryConstants(){}

    public static final String SAVINGS_JOIN_QUERY = """
            SELECT
              DISTINCT MDP.membershipId AS membershipUUID,
              MDP.cardNumber AS cardNumber,
              SUBSTR(MDP.cardNumber,3) AS membershipID,
              reco.modelType AS modelType,
              reco.rankedProducts AS rankedProducts
            FROM
              `prod-sams-mdp.mdp_secure.SAMS_UNIFIED_MEMBER_CARD` AS MDP
            JOIN
              `%s.temp_savings.SAVINGS_RECOMMENDATION`AS reco
            ON
              SUBSTR(MDP.cardNumber,3) = reco.cardNumber
            WHERE
              MDP.current_ind = 'Y'
              AND MDP.card_current_ind = 'Y'
              AND MDP.cardNumber IS NOT NULL
              AND MDP.membershipId IS NOT NULL
              AND (TRIM(MDP.cardStatus) = 'ACTIVE'
                OR TRIM(MDP.cardStatus) = 'PICKUP'
                OR TRIM(MDP.cardStatus) = 'EXPIRED')
              AND MDP.cardType = 'MEMBERSHIPACCOUNT'
              AND (MDP.ISTESTMEMBERSHIP IS NULL OR MDP.ISTESTMEMBERSHIP = 'false')
              AND TRIM(membershipRoleDesc)='PRIMARY'
              AND (MDP.UPDATEDFEATURE IS NULL
                OR MDP.UPDATEDFEATURE <> "HDP_MIGRATION")
              AND MDP.membershipStatus IN ( 'ACTIVE',
                'EXPIRED')
            """;


    public static final String LANDING_PAGE_QUERY = """
                           SELECT
                              DISTINCT MDP.membershipId AS membershipUUID,
                             MDP.cardNumber AS cardNumber,
                             SUBSTR(MDP.cardNumber,3) AS membershipId,
                             reco.modelType AS modelType,
                             reco.rankedProducts AS rankedProducts
                            FROM
                             `prod-sams-mdp.mdp_secure.SAMS_UNIFIED_MEMBER_CARD` AS MDP
                            JOIN
                             `%s.temp_landing_page.LANDING_PAGE_RECOMMENDATION`AS reco
                            ON
                             SUBSTR(MDP.cardNumber,3) = reco.cardNumber
                            WHERE
                             MDP.current_ind = 'Y'
                             AND MDP.card_current_ind = 'Y'
                             AND MDP.cardNumber IS NOT NULL
                             AND MDP.membershipId IS NOT NULL
                             AND (TRIM(MDP.cardStatus) = 'ACTIVE'
                               OR TRIM(MDP.cardStatus) = 'PICKUP'
                               OR TRIM(MDP.cardStatus) = 'EXPIRED')
                             AND MDP.cardType = 'MEMBERSHIPACCOUNT'
                             AND (MDP.ISTESTMEMBERSHIP IS NULL OR MDP.ISTESTMEMBERSHIP = 'false')
                             AND (MDP.UPDATEDFEATURE IS NULL
                               OR MDP.UPDATEDFEATURE <> "HDP_MIGRATION")
                             AND MDP.membershipStatus IN ( 'ACTIVE',
                               'EXPIRED')            
            """;

    public static final String RYE_QUERY = """
            SELECT
              DISTINCT MDP.membershipId AS membershipUUID,
              MDP.cardNumber AS cardNumber,
              SUBSTR(MDP.cardNumber,3) AS membershipID,
              reco.modelType AS modelType,
              reco.rankedProducts AS rankedProducts
            FROM
              `prod-sams-mdp.mdp_secure.SAMS_UNIFIED_MEMBER_CARD` AS MDP
            JOIN
              `%s.temp_rye.RYE_RECOMMENDATION`AS reco
            ON
              SUBSTR(MDP.cardNumber,3) = reco.cardNumber
            WHERE
              MDP.current_ind = 'Y'
              AND MDP.card_current_ind = 'Y'
              AND (TRIM(MDP.cardStatus) = 'ACTIVE'
                OR TRIM(MDP.cardStatus) = 'PICKUP'
                OR TRIM(MDP.cardStatus) = 'EXPIRED')
              AND MDP.cardType = 'MEMBERSHIPACCOUNT'
              AND (MDP.UPDATEDFEATURE IS NULL
                OR MDP.UPDATEDFEATURE <> 'HDP_MIGRATION')
              AND MDP.membershipStatus IN ( 'ACTIVE',
                'EXPIRED')
            """;


    public static final String DEFAULT_JOIN_QUERY = """
            SELECT
              DISTINCT MDP.membershipId AS membershipUUID,
              MDP.cardNumber AS cardNumber,
              SUBSTR(MDP.cardNumber,3) AS membershipID,
              reco.modelType AS modelType,
              reco.rankedProducts AS rankedProducts
            FROM
              `prod-sams-mdp.mdp_secure.SAMS_UNIFIED_MEMBER_CARD` AS MDP
            JOIN
              `%s.temp_default.DEFAULT_RECOMMENDATION`AS reco
            ON
              SUBSTR(MDP.cardNumber,3) = reco.cardNumber
            WHERE
              MDP.current_ind = 'Y'
              AND MDP.card_current_ind = 'Y'
              AND MDP.cardNumber IS NOT NULL
              AND MDP.membershipId IS NOT NULL
              AND (TRIM(MDP.cardStatus) = 'ACTIVE'
                OR TRIM(MDP.cardStatus) = 'PICKUP'
                OR TRIM(MDP.cardStatus) = 'EXPIRED')
              AND MDP.cardType = 'MEMBERSHIPACCOUNT'
              AND (MDP.ISTESTMEMBERSHIP IS NULL OR MDP.ISTESTMEMBERSHIP = 'false')
              AND TRIM(membershipRoleDesc)='PRIMARY'
              AND (MDP.UPDATEDFEATURE IS NULL
                OR MDP.UPDATEDFEATURE <> "HDP_MIGRATION")
              AND MDP.membershipStatus IN ( 'ACTIVE',
                'EXPIRED')
            """;

}
