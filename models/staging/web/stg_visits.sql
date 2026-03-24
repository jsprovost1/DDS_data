WITH source AS (
    SELECT * FROM {{ source('web', 'df_visit') }}
),

renamed AS (
    SELECT
        -- Foreign key
         CAST(user_id AS VARCHAR)       AS user_id,

        -- Visit attributes
        CAST(visit_date AS TIMESTAMP)   AS visit_date,
        webpages

    FROM source
)

SELECT * FROM renamed