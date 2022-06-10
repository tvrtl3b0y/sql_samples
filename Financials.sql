WITH 

-- CLEAN DATA SOURCES
revenue_from_payments AS (
    SELECT
        'Subscription Fees' AS revenue_stream,
        "createdAt" AT TIME ZONE 'PST' AS payment_created_at,
        "ownerId" AS user_id,
        "fundID" AS fund_id,
        amount * 0.01 AS revenue_usd
    FROM payments
    WHERE
        type = 'voluntary-fee'
        AND state IN('initiated', 'settled')
    
    UNION ALL 

    SELECT
        'Tips' AS revenue_stream,
        "createdAt" AT TIME ZONE 'PST' AS payment_created_at,
        "ownerId" AS user_id,
        "fundID" AS fund_id,
        tip * 0.01 AS revenue_usd
    FROM payments
    WHERE
        state = 'settled'

    UNION ALL

    SELECT
        'Processing Fees' AS revenue_stream,
        "createdAt" AT TIME ZONE 'PST' AS payment_created_at,
        "ownerId" AS user_id,
        "fundID" AS fund_id,
        "processingFee" * 0.01 AS revenue_usd
    FROM payments
    WHERE
        state = 'settled'
),

revenue_from_backer_bucks AS (
    SELECT
        'Backer Payouts' AS revenue_stream,
        al."createdAt" AT TIME ZONE 'PST' AS payment_created_at,
        al."userId" AS user_id,
        ac."fundVoucherId" AS fund_id,
        ac.state AS commission_state,
        ac.payout * 0.01 AS revenue_usd
    FROM affiliate_links al
    LEFT JOIN affiliate_commissions ac
    ON al.id = ac."affiliateLinkId"
    WHERE
        ac.description IS NOT NULL
        AND ac."userId" <> '1'
),

-- AGGREGATE
revenue_by_stream AS (
    SELECT
        revenue_stream,
        payment_created_at,
        user_id,
        fund_id,
        revenue_usd
    FROM revenue_from_payments

    UNION ALL

    SELECT 
        revenue_stream,
        payment_created_at,
        user_id,
        fund_id,
        revenue_usd
    FROM revenue_from_backer_bucks
),

paying_users AS (
    SELECT
        u.user_id,
        u.is_owner,
        u.utm_source,
        p.revenue_stream,
        p.payment_created_at,
        p.user_revenue_usd
    FROM (
        SELECT DISTINCT
            uu.id AS user_id,
            uu."utmSource" AS utm_source,
            CASE WHEN f."owner" IS NOT NULL THEN TRUE ELSE FALSE END AS is_owner
        FROM users uu
        LEFT JOIN funds f
        ON uu.id = f."owner"
    ) u
    LEFT JOIN (
        SELECT
            user_id,
            payment_created_at,
            revenue_stream,
            SUM(revenue_usd) AS user_revenue_usd
        FROM revenue_by_stream
        GROUP BY 1, 2, 3
    )  p
    ON u.user_id = p.user_id
)

SELECT
    user_id,
    is_owner,
    utm_source,
    revenue_stream,
    payment_created_at,
    CASE WHEN utm_source = 'foundation' THEN TRUE ELSE FALSE END AS is_foundation,
    user_revenue_usd
FROM paying_users