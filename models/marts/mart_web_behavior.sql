WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

visits AS (
    SELECT * FROM {{ ref('stg_visits') }}
),

conversions AS (
    SELECT * FROM {{ ref('stg_conversions') }}
),

-- -----------------------------------------------
-- Visits BEFORE signup per user
-- -----------------------------------------------
visits_before_signup AS (
    SELECT
        v.user_id,
        COUNT(*)                            AS total_visits_before_signup,
        MIN(v.visit_date)                   AS first_visit_date,
        MAX(v.visit_date)                   AS last_visit_before_signup
    FROM visits v
    INNER JOIN users u ON v.user_id = u.user_id
    WHERE v.visit_date <= u.signup_date
    GROUP BY v.user_id
),

-- -----------------------------------------------
-- First visit details per user
-- -----------------------------------------------
first_visit AS (
    SELECT
        user_id,
        visit_date                          AS first_visit_date,
        webpages                            AS first_visit_page
    FROM visits
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY visit_date ASC
    ) = 1
),

-- -----------------------------------------------
-- All visits with conversion flag
-- -----------------------------------------------
visits_with_conversion AS (
    SELECT
        v.user_id,
        v.visit_date,
        v.webpages,
        DATE(v.visit_date)                  AS visit_day,
        DATE_TRUNC('month', v.visit_date)   AS visit_month,
        CASE
            WHEN c.user_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END                                 AS converted
    FROM visits v
    LEFT JOIN conversions c ON v.user_id = c.user_id
),

-- -----------------------------------------------
-- Webpage level metrics
-- -----------------------------------------------
webpage_metrics AS (
    SELECT
        webpages,
        COUNT(*)                            AS total_visits,
        COUNT(DISTINCT user_id)             AS unique_visitors,
        SUM(CASE WHEN converted THEN 1
            ELSE 0 END)                     AS converting_visits,
        ROUND(
            SUM(CASE WHEN converted THEN 1
                ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                   AS conversion_rate
    FROM visits_with_conversion
    GROUP BY webpages
),

-- -----------------------------------------------
-- User level visit summary
-- -----------------------------------------------
user_visit_summary AS (
    SELECT
        user_id,
        COUNT(*)                            AS total_visits,
        COUNT(DISTINCT DATE(visit_date))    AS unique_visit_days,
        MIN(visit_date)                     AS first_visit_date,
        MAX(visit_date)                     AS last_visit_date,
        COUNT(DISTINCT webpages)            AS unique_pages_visited,
        DATEDIFF('day',
            MIN(visit_date),
            MAX(visit_date)
        )                                   AS days_between_first_last_visit
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
        u.signup_date,
        u.login_duration,

        -- -----------------------------------------------
        -- Visit summary
        -- -----------------------------------------------
        uvs.total_visits,
        uvs.unique_visit_days,
        uvs.unique_pages_visited,
        uvs.first_visit_date,
        uvs.last_visit_date,
        uvs.days_between_first_last_visit,

        -- -----------------------------------------------
        -- Visits before signup
        -- -----------------------------------------------
        COALESCE(vbs.total_visits_before_signup, 0)
                                            AS visits_before_signup,
        vbs.last_visit_before_signup,

        -- -----------------------------------------------
        -- First visit details
        -- -----------------------------------------------
        fv.first_visit_date                 AS first_visit_timestamp,
        fv.first_visit_page,

        -- Days from first visit to signup
        DATEDIFF('day',
            DATE(fv.first_visit_date),
            DATE(u.signup_date)
        )                                   AS days_first_visit_to_signup,

        -- -----------------------------------------------
        -- Login duration on first visit
        -- -----------------------------------------------
        u.login_duration                    AS first_visit_login_duration,

        -- -----------------------------------------------
        -- Conversion flag
        -- -----------------------------------------------
        CASE
            WHEN u.trial_date IS NOT NULL THEN TRUE
            ELSE FALSE
        END                                 AS converted_to_trial

    FROM users u
    LEFT JOIN user_visit_summary uvs    ON u.user_id = uvs.user_id
    LEFT JOIN visits_before_signup vbs  ON u.user_id = vbs.user_id
    LEFT JOIN first_visit fv            ON u.user_id = fv.user_id
)

SELECT * FROM final