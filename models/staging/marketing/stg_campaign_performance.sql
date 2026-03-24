WITH source AS (
    SELECT * FROM {{ source('marketing', 'df_campaign') }}
),

renamed AS (
    SELECT
        -- Foreign key
        CAST(campaign_id AS VARCHAR)        AS campaign_id,

        -- Performance attributes
        CAST(year_month_day AS DATE)        AS year_month_day,
        impressions,
        clicks,
        ROUND(CAST(cost AS FLOAT), 2)       AS cost

    FROM source
)

SELECT * FROM renamed