WITH source AS (
    SELECT * FROM {{ source('email', 'df_bounced') }}
),

renamed AS (
    SELECT
        -- Foreign key
        CAST(user_id AS VARCHAR)            AS user_id,

        -- Email attributes
        LOWER(TRIM(emails))                 AS emails,
        CAST(bounced_email AS TIMESTAMP)    AS bounced_email

    FROM source
)

SELECT * FROM renamed