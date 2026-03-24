WITH source AS (
    SELECT * FROM {{ source('email', 'df_clicked') }}
),

renamed AS (
    SELECT
        -- Foreign key
        CAST(user_id AS VARCHAR)            AS user_id,

        -- Email attributes
        LOWER(TRIM(emails))                 AS emails,
        CAST(clicked_email AS TIMESTAMP)    AS clicked_email

    FROM source
)

SELECT * FROM renamed