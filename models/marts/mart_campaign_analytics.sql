WITH campaigns AS (
    SELECT * FROM {{ ref('stg_campaigns') }}
),

performance AS (
    SELECT * FROM {{ ref('stg_campaign_performance') }}
),

conversions AS (
    SELECT * FROM {{ ref('stg_conversions') }}
),

-- Aggregate performance per campaign per day
campaign_daily AS (
    SELECT
        campaign_id,
        year_month_day,
        SUM(impressions)                AS daily_impressions,
        SUM(clicks)                     AS daily_clicks,
        SUM(cost)                       AS daily_cost
    FROM performance
    GROUP BY campaign_id, year_month_day
),

-- Aggregate conversions per campaign
campaign_conversions AS (
    SELECT
        LOWER(TRIM(campaign_name))      AS campaign_name,
        COUNT(*)                        AS total_conversions,
        COUNT(DISTINCT user_id)         AS unique_users_converted
    FROM conversions
    WHERE campaign_name IS NOT NULL
    GROUP BY LOWER(TRIM(campaign_name))
),

final AS (
    SELECT
        -- Campaign identity
        c.campaign_id,
        c.campaign_name,
        c.campaign_status,

        -- Daily performance
        cd.year_month_day,
        cd.daily_impressions,
        cd.daily_clicks,
        cd.daily_cost,

        -- Conversion metrics
        COALESCE(cc.total_conversions, 0)       AS total_conversions,
        COALESCE(cc.unique_users_converted, 0)  AS unique_users_converted,

        -- Derived metrics
        CASE
            WHEN cd.daily_impressions > 0
            THEN ROUND(cd.daily_clicks / cd.daily_impressions * 100, 4)
            ELSE 0
        END                                     AS click_through_rate,

        CASE
            WHEN cd.daily_clicks > 0
            THEN ROUND(cd.daily_cost / cd.daily_clicks, 2)
            ELSE 0
        END                                     AS cost_per_click,

        CASE
            WHEN COALESCE(cc.total_conversions, 0) > 0
            THEN ROUND(cd.daily_cost / cc.total_conversions, 2)
            ELSE 0
        END                                     AS cost_per_conversion

    FROM campaigns c
    LEFT JOIN campaign_daily cd         ON c.campaign_id = cd.campaign_id
    LEFT JOIN campaign_conversions cc   ON c.campaign_name = cc.campaign_name
)

SELECT * FROM final