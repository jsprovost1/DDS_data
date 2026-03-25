WITH source AS (
    SELECT * FROM {{ source('users', 'df_users') }}
),

renamed AS (
    SELECT
        -- Primary key
        CAST(user_id AS VARCHAR(9))  AS user_id,

        -- User attributes
        CAST(email AS VARCHAR)              AS email,
        CAST(phone AS INTEGER)              AS phone,
        CAST(age AS INTEGER)                AS age,
        CAST(registration_type AS VARCHAR)  AS registration_type,
        CAST(user_bot AS INTEGER)           AS user_bot,
        CAST(internal_user AS INTEGER)      AS internal_user,
        CAST(login_duration AS FLOAT)      AS login_duration,

        -- Dates
        CAST(signup_date AS TIMESTAMP)      AS signup_date,
        CAST(signup_date_new AS DATE)       AS signup_date_clean,
        CAST(trial_date AS TIMESTAMP)       AS trial_date,

        -- Subscription dates
        CAST(subscr_3m_date AS DATE)        AS subscr_3m_date,
        CAST(subscr_12m_date AS DATE)       AS subscr_12m_date,
        marketing_12m_subscr

    FROM source
    WHERE user_bot = FALSE          -- exclude bots
      AND internal_user = 0         -- exclude internal users
)

SELECT * FROM renamed