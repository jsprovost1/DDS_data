WITH source AS (
    SELECT * FROM {{ source('email', 'df_spam') }}
),

renamed AS (
    SELECT
        -- Foreign key
        CAST(user_id AS VARCHAR)            AS user_id,

        -- Email attributes
        LOWER(TRIM(emails))                 AS emails,
        CAST(spam_email AS TIMESTAMP)       AS spam_email

    FROM source
)

SELECT * FROM renamed