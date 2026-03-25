WITH sent AS (
    SELECT
        user_id,
        emails,
        sent_email          AS event_timestamp,
        'sent'              AS email_status
    FROM {{ ref('stg_email_sent') }}
),

delivered AS (
    SELECT
        user_id,
        emails,
        delivered_email     AS event_timestamp,
        'delivered'         AS email_status
    FROM {{ ref('stg_email_delivered') }}
),

opened AS (
    SELECT
        user_id,
        emails,
        open_email          AS event_timestamp,
        'opened'            AS email_status
    FROM {{ ref('stg_email_opened') }}
),

clicked AS (
    SELECT
        user_id,
        emails,
        clicked_email       AS event_timestamp,
        'clicked'           AS email_status
    FROM {{ ref('stg_email_clicked') }}
),

bounced AS (
    SELECT
        user_id,
        emails,
        bounced_email       AS event_timestamp,
        'bounced'           AS email_status
    FROM {{ ref('stg_email_bounced') }}
),

spam AS (
    SELECT
        user_id,
        emails,
        spam_email          AS event_timestamp,
        'spam'              AS email_status
    FROM {{ ref('stg_email_spam') }}
),

unioned AS (
    SELECT * FROM sent
    UNION ALL SELECT * FROM delivered
    UNION ALL SELECT * FROM opened
    UNION ALL SELECT * FROM clicked
    UNION ALL SELECT * FROM bounced
    UNION ALL SELECT * FROM spam
),

final AS (
    SELECT
        user_id,
        emails,
        email_status,
        event_timestamp,
        DATE(event_timestamp)   AS event_date
    FROM unioned
)

SELECT * FROM final