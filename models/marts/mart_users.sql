WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

conversions AS (
    SELECT * FROM {{ ref('stg_conversions') }}
),

visits AS (
    SELECT * FROM {{ ref('stg_visits') }}
),

-- Aggregate conversions per user
user_conversions AS (
    SELECT
        user_id,
        COUNT(*)                        AS total_conversions,
        MIN(signup_date)                AS first_conversion_date,
        SUM(CASE WHEN conversion_type = 'paid'
            THEN 1 ELSE 0 END)          AS paid_conversions,
        SUM(CASE WHEN conversion_type = 'organic'
            THEN 1 ELSE 0 END)          AS organic_conversions,
        MAX(campaign_name)              AS last_campaign
    FROM conversions
    GROUP BY user_id
),

-- Aggregate visits per user
user_visits AS (
    SELECT
        user_id,
        COUNT(*)                        AS total_visits,
        MIN(visit_date)                 AS first_visit_date,
        MAX(visit_date)                 AS last_visit_date
    FROM visits
    GROUP BY user_id
),

final AS (
    SELECT
        -- User identity
        u.user_id,
        u.email,
        u.age,
        u.registration_type,

        -- Signup info
        u.signup_date,
        u.signup_date_clean,
        u.trial_date,
        u.subscr_3m_date,
        u.subscr_12m_date,
        u.marketing_12m_subscr,

        -- Conversion metrics
        COALESCE(uc.total_conversions, 0)       AS total_conversions,
        COALESCE(uc.paid_conversions, 0)        AS paid_conversions,
        COALESCE(uc.organic_conversions, 0)     AS organic_conversions,
        uc.first_conversion_date,
        uc.last_campaign,

        -- Acquisition type
        CASE
            WHEN COALESCE(uc.paid_conversions, 0) > 0 THEN 'paid'
            ELSE 'organic'
        END                                     AS acquisition_type,

        -- Days from signup to first conversion
        DATEDIFF('day',
            u.signup_date,
            uc.first_conversion_date
        )                                       AS days_to_conversion,

        -- Visit metrics
        COALESCE(uv.total_visits, 0)            AS total_visits,
        uv.first_visit_date,
        uv.last_visit_date,

        -- Days between first and last visit
        DATEDIFF('day',
            uv.first_visit_date,
            uv.last_visit_date
        )                                       AS days_active

    FROM users u
    LEFT JOIN user_conversions uc   ON u.user_id = uc.user_id
    LEFT JOIN user_visits uv        ON u.user_id = uv.user_id
)

SELECT * FROM final