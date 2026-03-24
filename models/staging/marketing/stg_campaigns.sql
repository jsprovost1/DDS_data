WITH source AS (
    SELECT * FROM {{ source('marketing', 'df_campaigns') }}
),

renamed AS (
    SELECT
        -- Not Primary key
        CAST(campaign_id AS VARCHAR)        AS campaign_id,

        -- Campaign attributes
        LOWER(TRIM(campaign_name))          AS campaign_name,
        LOWER(TRIM(campaign_status))        AS campaign_status

    FROM source
)

SELECT * FROM renamed