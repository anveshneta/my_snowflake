-- models/revenue_by_egm_and_venue.sql
SELECT
    EGM_DESCRIPTION,
    VENUE_CODE,
    SUM(GMP_SUM) AS TOTAL_REVENUE
FROM {{ source('GAME_SCHEMA', 'GAME_PERFORMANCE_DATA') }}
GROUP BY EGM_DESCRIPTION, VENUE_CODE
