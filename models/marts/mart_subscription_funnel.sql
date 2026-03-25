WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

conversions AS (
    SELECT * FROM {{ ref('stg_conversions') }}
),

-- Get first paid conversion per user with campaign
paid_conversions AS (
    SELECT
        user_id,
        MIN(signup_date)            AS first_paid_date,
        MIN(campaign_name)          AS first_paid_campaign
    FROM conversions
    WHERE conversion_type = 'paid'
    GROUP BY user_id
),

-- Get first organic conversion per user
organic_conversions AS (
    SELECT
        user_id,
        MIN(signup_date)            AS first_organic_date
    FROM conversions
    WHERE conversion_type = 'organic'
    GROUP BY user_id
),

final AS (
    SELECT
        -- User identity
        u.user_id,
        u.email,
        u.age,
        u.registration_type,
        u.marketing_12m_subscr,

        -- -----------------------------------------------
        -- STAGE 1 — Signup
        -- -----------------------------------------------
        u.signup_date,
        DATE(u.signup_date)                         AS signup_day,
        DATE_TRUNC('month', u.signup_date)          AS signup_month,
        DATE_TRUNC('year', u.signup_date)           AS signup_year,

        -- -----------------------------------------------
        -- STAGE 2 — Free trial
        -- -----------------------------------------------
        u.trial_date,
        CASE
            WHEN u.trial_date IS NOT NULL THEN TRUE
            ELSE FALSE
        END                                         AS has_trial,

        -- Days from signup to trial
        DATEDIFF('day',
            DATE(u.signup_date),
            DATE(u.trial_date)
        )                                           AS days_signup_to_trial,

        -- -----------------------------------------------
        -- STAGE 3 — 3-month subscription
        -- -----------------------------------------------
        u.subscr_3m_date,
        CASE
            WHEN u.subscr_3m_date IS NOT NULL THEN TRUE
            ELSE FALSE
        END                                         AS has_3m_subscription,

        -- Days from trial to 3-month subscription
        DATEDIFF('day',
            DATE(u.trial_date),
            CAST(u.subscr_3m_date AS DATE)
        )                                           AS days_trial_to_3m,

        -- Days from signup to 3-month subscription
        DATEDIFF('day',
            DATE(u.signup_date),
            CAST(u.subscr_3m_date AS DATE)
        )                                           AS days_signup_to_3m,

        -- -----------------------------------------------
        -- STAGE 4 — 12-month subscription
        -- -----------------------------------------------
        u.subscr_12m_date,
        CASE
            WHEN u.subscr_12m_date IS NOT NULL THEN TRUE
            ELSE FALSE
        END                                         AS has_12m_subscription,

        -- Days from 3-month to 12-month subscription
        DATEDIFF('day',
            CAST(u.subscr_3m_date AS DATE),
            CAST(u.subscr_12m_date AS DATE)
        )                                           AS days_3m_to_12m,

        -- Days from signup to 12-month subscription (full journey)
        DATEDIFF('day',
            DATE(u.signup_date),
            CAST(u.subscr_12m_date AS DATE)
        )                                           AS days_signup_to_12m,

        -- -----------------------------------------------
        -- CONVERSION INFO
        -- -----------------------------------------------
        pc.first_paid_campaign,

        CASE
            WHEN pc.user_id IS NOT NULL THEN 'paid'
            WHEN oc.user_id IS NOT NULL THEN 'organic'
            ELSE 'unknown'
        END                                         AS acquisition_type,

        -- -----------------------------------------------
        -- FUNNEL STAGE (furthest stage reached)
        -- -----------------------------------------------
        CASE
            WHEN u.subscr_12m_date IS NOT NULL THEN '4_yearly'
            WHEN u.subscr_3m_date IS NOT NULL  THEN '3_monthly'
            WHEN u.trial_date IS NOT NULL       THEN '2_trial'
            ELSE '1_signup'
        END                                         AS funnel_stage,

        -- -----------------------------------------------
        -- ACTIVE STATUS (based on subscription dates)
        -- -----------------------------------------------
        CASE
            WHEN u.subscr_12m_date IS NOT NULL
                AND DATEADD('month', 12,
                    CAST(u.subscr_12m_date AS DATE)
                ) >= CURRENT_DATE                   THEN 'active_yearly'
            WHEN u.subscr_3m_date IS NOT NULL
                AND DATEADD('month', 3,
                    CAST(u.subscr_3m_date AS DATE)
                ) >= CURRENT_DATE                   THEN 'active_monthly'
            WHEN u.trial_date IS NOT NULL
                AND DATEADD('day', 7,
                    DATE(u.trial_date)
                ) >= CURRENT_DATE                   THEN 'active_trial'
            ELSE 'churned'
        END                                         AS subscription_status

    FROM users u
    LEFT JOIN paid_conversions pc       ON u.user_id = pc.user_id
    LEFT JOIN organic_conversions oc    ON u.user_id = oc.user_id
)

SELECT * FROM final