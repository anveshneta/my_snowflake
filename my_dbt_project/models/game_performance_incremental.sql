{{
    config(
        materialized='incremental',
        alias='GAME_PERFORMANCE_DATA',
        unique_key='BUS_DATE'
    )
}}

-- Determine the maximum date in the target table during an incremental run
WITH latest_bus_date AS (
    {% if is_incremental() %}
        SELECT MAX(BUS_DATE) AS MAX_DATE
        FROM {{ this }}
    {% else %}
        SELECT NULL AS MAX_DATE
    {% endif %}
),

-- Fetch new or updated rows based on BUS_DATE
incremental_data AS (
    SELECT
        BUS_DATE,
        VENUE_CODE,
        EGM_DESCRIPTION,
        MANUFACTURER,
        FP,
        TURNOVER_SUM,
        GMP_SUM,
        GAMES_PLAYED_SUM
    FROM {{ source('GAME_SCHEMA', 'GAME_PERFORMANCE_INCREMENTAL') }}
    WHERE BUS_DATE > (SELECT MAX_DATE FROM latest_bus_date)
)

-- Combine all rows (existing and new/updated)
SELECT *
FROM incremental_data
