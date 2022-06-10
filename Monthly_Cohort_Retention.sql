-- Light-weight version of month-over-month retention

WITH

-- CLEAN DATA SOURCES
payments_cleaned AS (
    SELECT 
        -- Payment Information
        id AS payment_id,
        "ownerId" AS user_id,
        "fundID" AS fund_id,
        "contributionID" AS contribution_id,
        "migratedWithPayment" AS payment_migrated_with_payment,
        
        -- Payment Amounts
        COALESCE(amount * 0.01, 0) AS payment_amount_usd,
        COALESCE("processingFee" * 0.01, 0) AS payment_processing_fee_usd,
        COALESCE("subscriptionFee" * 0.01, 0) AS payment_subscription_fee_usd,
        COALESCE(tip * 0.01, 0) AS payment_tip_usd,
        
        -- Payment Dates
        "createdAt" AT TIME ZONE 'PST' AS payment_created_at,
        DATE("transactionDate") AT TIME ZONE 'PST' payment_transaction_date,
        DATE("dueDate") AT TIME ZONE 'PST' AS payment_due_date,
        "updatedAt" AT TIME ZONE 'PST' AS payment_updated_at,
        "deletedAt" AT TIME ZONE 'PST' AS payment_deleted_at,
        
        -- Payment Dates (UTC)
        "createdAt" AS payment_created_at_utc,
        DATE("transactionDate") payment_transaction_date_utc,
        DATE("dueDate") AS payment_due_date_utc,
        "updatedAt" AS payment_updated_at_utc,
        "deletedAt" AS payment_deleted_at_utc,
        
        -- Payment ACH Dates
        "initiatedAt" AT TIME ZONE 'PST' AS payment_initiated_at,
        "pullSettledAt" AT TIME ZONE 'PST' AS payment_pull_settled_at,
        "pushedAt" AT TIME ZONE 'PST' AS payment_pushed_at,
        "pushSettledAt" AT TIME ZONE 'PST' AS payment_push_settled_at,
        "settledAt" AT TIME ZONE 'PST' AS payment_settled_at,
        "refundedAt" AT TIME ZONE 'PST' AS payment_refunded_at,
        "cancelRequestedAt" AT TIME ZONE 'PST' AS payment_cancel_requested_at,
        "cancelledAt" AT TIME ZONE 'PST' AS payment_cancelled_at,
        
        -- Payment ACH Dates
        "initiatedAt" AS payment_initiated_at_utc,
        "pullSettledAt" AS payment_pull_settled_at_utc,
        "pushedAt" AS payment_pushed_at_utc,
        "pushSettledAt" AS payment_push_settled_at_utc,
        "settledAt" AS payment_settled_at_utc,
        "refundedAt" AS payment_refunded_at_utc,
        "cancelRequestedAt" AS payment_cancel_requested_at_utc,
        "cancelledAt" AS payment_cancelled_at_utc,
        
        -- Payment Type
        type AS payment_type,
        
        -- Payment Method Info
        "bankAccountId" AS payment_bank_account_id,
        "creditCardId" AS payment_credit_card_id,
        CASE 
            WHEN type IN ('authorize-net', 'uesp') THEN 'Legacy'
            WHEN type IN ('migration-to-my529', 'scholarshare', 'voluntary-fee', 'rollover-earnings', 'rollover-contribution') THEN 'Operational'
            WHEN type LIKE 'ach%' THEN 'ACH'
            WHEN type IN ('stripe', 'stripecard') THEN 'Credit Card'
        END AS payment_method_type,
        CASE
            WHEN type = 'voluntary-fee' THEN 'Subscription Fee'
            WHEN "contributionID" IS NOT NULL THEN 'Contribution Payment'
            ELSE 'Unknown'
        END AS payment_is_for,
        
        -- Payment Status
        "retries" AS payment_retries,
        "settlementMethod" AS payment_settlement_method,
        state AS payment_state
    FROM payments
),

-- AGGREGATE
activity_by_users AS (
    SELECT DISTINCT
        user_id,
        DATE_TRUNC('month', payment_created_at) AS payment_created_at
    FROM payments_cleaned
    WHERE
        contribution_id IS NOT NULL
        AND payment_state IN ('settled', 'initiated')
        AND payment_created_at BETWEEN '2021-01-01' AND DATE_TRUNC('month', NOW() AT TIME ZONE 'PST')
),

base AS (
  SELECT
    a.user_id,
    a.payment_created_at AS activity,
    r.payment_created_at AS futureActivity,
    EXTRACT(year FROM AGE(r.payment_created_at, a.payment_created_at)) * 12  + EXTRACT(month FROM AGE(r.payment_created_at, a.payment_created_at)) AS diff
  FROM activity_by_users a
  LEFT JOIN activity_by_users r
  ON 
    a.user_id = r.user_id
    AND r.payment_created_at >= a.payment_created_at
),

base2 AS (
    SELECT 
        activity,
        diff,
        COUNT(DISTINCT user_id) AS retained
    FROM base
    GROUP BY 1, 2
    ORDER BY 1, 2
)

SELECT
    activity,
    diff,
    retained,
    FIRST_VALUE(retained) OVER (
        PARTITION BY activity
        ORDER BY diff
    ) AS total,
    retained * 1.0 / FIRST_VALUE(retained) OVER (
        PARTITION BY activity
        ORDER BY diff
    ) AS retention_rate
FROM base2