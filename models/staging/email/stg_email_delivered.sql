WITH source AS (
    SELECT * FROM {{ source('email', 'df_delivered') }}
),

renamed AS (
    SELECT
        -- Foreign key
        CAST(user_id AS VARCHAR)            AS user_id,

        -- Email attributes
        LOWER(TRIM(emails))                 AS emails,
        CAST(delivered_email AS TIMESTAMP)  AS delivered_email

    FROM source
)

SELECT * FROM renamed