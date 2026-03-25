WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

email_funnel AS (
    SELECT * FROM {{ ref('int_email_funnel') }}
),

-- Pivot email funnel metrics per user
user_email_metrics AS (
    SELECT
        user_id,
        emails,

        -- Event counts
        SUM(CASE WHEN email_status = 'sent'
            THEN 1 ELSE 0 END)          AS total_sent,
        SUM(CASE WHEN email_status = 'delivered'
            THEN 1 ELSE 0 END)          AS total_delivered,
        SUM(CASE WHEN email_status = 'opened'
            THEN 1 ELSE 0 END)          AS total_opened,
        SUM(CASE WHEN email_status = 'clicked'
            THEN 1 ELSE 0 END)          AS total_clicked,
        SUM(CASE WHEN email_status = 'bounced'
            THEN 1 ELSE 0 END)          AS total_bounced,
        SUM(CASE WHEN email_status = 'spam'
            THEN 1 ELSE 0 END)          AS total_spam,

        -- First and last event dates
        MIN(CASE WHEN email_status = 'sent'
            THEN event_date END)        AS first_sent_date,
        MAX(CASE WHEN email_status = 'sent'
            THEN event_date END)        AS last_sent_date

    FROM email_funnel
    GROUP BY user_id, emails
),

final AS (
    SELECT
        -- User identity
        u.user_id,
        u.email,
        u.registration_type,
        u.signup_date_clean,

        -- Email campaign
        uem.emails                      AS email_campaign,

        -- Email funnel counts
        uem.total_sent,
        uem.total_delivered,
        uem.total_opened,
        uem.total_clicked,
        uem.total_bounced,
        uem.total_spam,

        -- Email dates
        uem.first_sent_date,
        uem.last_sent_date,

        -- Email funnel rates
        CASE
            WHEN uem.total_sent > 0
            THEN ROUND(uem.total_delivered / uem.total_sent * 100, 2)
            ELSE 0
        END                             AS delivery_rate,

        CASE
            WHEN uem.total_delivered > 0
            THEN ROUND(uem.total_opened / uem.total_delivered * 100, 2)
            ELSE 0
        END                             AS open_rate,

        CASE
            WHEN uem.total_opened > 0
            THEN ROUND(uem.total_clicked / uem.total_opened * 100, 2)
            ELSE 0
        END                             AS click_to_open_rate,

        CASE
            WHEN uem.total_sent > 0
            THEN ROUND(uem.total_bounced / uem.total_sent * 100, 2)
            ELSE 0
        END                             AS bounce_rate,

        CASE
            WHEN uem.total_sent > 0
            THEN ROUND(uem.total_spam / uem.total_sent * 100, 2)
            ELSE 0
        END                             AS spam_rate

    FROM users u
    INNER JOIN user_email_metrics uem   ON u.user_id = uem.user_id
)

SELECT * FROM final