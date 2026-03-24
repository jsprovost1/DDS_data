WITH source AS (
    SELECT * FROM {{ source('web', 'df_conversion') }}
),

renamed AS (
    SELECT
        -- Foreign key
        CAST(user_id AS VARCHAR)            AS user_id,

        -- Conversion attributes
        CAST(signup_date AS TIMESTAMP)      AS signup_date,
        LOWER(conversion_type)              AS conversion_type,
        LOWER(campaign_name)                AS campaign_name,
        CAST(year_month_day AS DATE)        AS year_month_day

    FROM source
)

SELECT * FROM renamed