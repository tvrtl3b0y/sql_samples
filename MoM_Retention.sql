WITH

-- CLEAN DATA SOURCES
funds_cleaned AS (
    SELECT 
        -- PII
        f.id AS fund_id,
        f.uuid AS fund_uuid,
        f.first AS beneficiary_first_name,
        f.middle AS beneficiary_middle_name,
        f.last AS beneficiary_last_name,
        TRIM(REPLACE(REPLACE(REPLACE(CONCAT(f.first, ' ', f.last), ' ', '<>'), '><', ''), '<>', ' ')) AS beneficiary_full_name,
        TRIM(REPLACE(REPLACE(REPLACE(CONCAT(f.first, ' ', f.middle, ' ', f.last), ' ', '<>'), '><', ''), '<>', ' ')) AS beneficiary_full_name_middle,
        f.dob AS beneficiary_dob,
        AGE(NOW(), f.dob) AS beneficiary_age,
        EXTRACT(year FROM AGE(NOW(), f.dob)) AS beneficiary_age_years,
        EXTRACT(month FROM AGE(NOW(), f.dob)) AS beneficiary_age_months,
        photo AS fund_photo,
        handle AS fund_handle,
        CASE 
            -- Special Cases
            WHEN description ILIKE '%with your help%' THEN 'with your help...' 
            WHEN description ILIKE '%backer is a smart way to save for college by inviting family and friends to contribute%' THEN 'backer is a smart way...'
            WHEN description ILIKE '%ahoy there!%' THEN 'ahoy there...'
            WHEN description ILIKE '%we hope to provide%with an opportunity to pursue the education%' THEN 'we hope to provide...'
            WHEN description ILIKE '%dear all%i%d like to invite you to contribute to %' THEN 'dear all...'
            WHEN description ILIKE '%i%m super excited to tell you about a new fund we set up for%college education%' THEN 'hi friend...'
            ELSE TRIM(LOWER(description))
        END AS fund_description,
        
        -- Dates
        f."createdAt" AT TIME ZONE 'PST' AS fund_created_at,
        f."updatedAt" AT TIME ZONE 'PST' AS fund_updated_at,
        f."deletedAt" AT TIME ZONE 'PST' AS fund_deleted_at,
        f."abandonedAt" AT TIME ZONE 'PST' AS fund_abandoned_at,
        f."establishedAt" AT TIME ZONE 'PST' AS fund_established_at,
        f."fundedAt" AT TIME ZONE 'PST' AS fund_funded_at,
        f."firstOneTimePaymentSettledAt" AT TIME ZONE 'PST' AS first_fund_onetime_payment_settled_at,
        f."firstRecurringPaymentSettledAt" AT TIME ZONE 'PST' AS first_fund_recurring_payment_settled_at,
        f."firstFamilyFundOn" AT TIME ZONE 'PST' AS first_family_fund_on,
        f."firstFamilyPaymentOn" AT TIME ZONE 'PST' AS first_family_payment_on,
        
        -- 529 Dates
        f."upgradeStartedAt" AT TIME ZONE 'PST' AS my529_upgrade_started_at,
        f."upgradeEstablishedAt" AT TIME ZONE 'PST' AS my529_upgrade_established_at,
        
        -- 529 Upgrade
        f."upgradeStartedAt" IS NOT NULL AS my529_upgrade_started_at_set,
        f."upgradeEstablishedAt" IS NOT NULL AS my529_upgrade_established_at_set,

        -- Fund Info
        f."fundName" AS fund_name,
        f.owner AS fund_owner_user_id,
        f."collegeSavingsBalance" * 0.01 AS fund_balance_usd,
        CASE 
            WHEN COALESCE(f."collegeSavingsBalance", 0) * 0.01 = 0 THEN '00: $0'
            WHEN f."collegeSavingsBalance" * 0.01 < 100 THEN '01: $1-$99'
            WHEN f."collegeSavingsBalance" * 0.01 < 500 THEN '02: $100-$499'
            WHEN f."collegeSavingsBalance" * 0.01 < 1000 THEN '03: $500-$999'
            WHEN f."collegeSavingsBalance" * 0.01 < 5000 THEN '04: $1,000-$4,999'
            WHEN f."collegeSavingsBalance" * 0.01 < 10000 THEN '05: $5,000-$9,999'
            WHEN f."collegeSavingsBalance" * 0.01 < 15000 THEN '06: $10,000-$14,999'
            WHEN f."collegeSavingsBalance" * 0.01 < 50000 THEN '07: $15,000-$49,999'
            WHEN f."collegeSavingsBalance" * 0.01 >= 50000 THEN '08: $50,000+'
        END AS fund_balance_group,
        f.status AS fund_status,
        f."lpoaStatus" AS fund_lpoa_status,
        f."statusReason" AS fund_status_reason,
        f."accountNumber" AS fund_account_number,
        f."feeBalance" * 0.01 AS fee_balance_usd,
        f.pricing AS fund_pricing,
        f.gender AS fund_gender,
        f.status <> 'inactive' AS fund_is_active,
        f."coverUrl" AS fund_cover_photo,
        TO_JSON(f.backers) AS fund_backers,
        JSON_ARRAY_LENGTH(TO_JSON(f.backers)) AS num_fund_backers,
        
        -- Starter Gift Info
        CASE 
            WHEN f."fromGift" IS NOT NULL THEN TRUE
            ELSE FALSE 
        END AS fund_is_from_gift,
        f."fromGift" AS from_gift_id,
        
        -- Fund Plan Info
        f."planId" AS fund_plan_id,
        f.plan AS fund_plan_type,
        f."programManager" AS fund_plan_manager,
        f.portfolio AS fund_plan_investment_portfolio,
        
        -- Fund Prebirth Info
        f."expectedDob" AS fund_expected_dob,
        f.prebirth AS fund_prebirth_status
    
    FROM funds f
),

transactions_cleaned AS (
    SELECT 
        -- Transaction Info
        t.id AS transaction_id,
        t."paymentID" AS payment_id,
        t.type AS transaction_type_detail,
        t.state AS transaction_state,
        
        -- Transaction Dates
        t."createdAt" AT TIME ZONE 'PST' AS transaction_created_at,
        t."updatedAt" AT TIME ZONE 'PST' AS transaction_updated_at,
        DATE(t."date") AT TIME ZONE 'PST' AS transaction_date,
        t."settledAt" AT TIME ZONE 'PST' AS transaction_settled_at
    
    FROM transactions t
),

cc_ach_transactions AS (
    WITH

    cc_transactions AS (
        SELECT 
            "createdAt" AT TIME ZONE 'PST' AS cc_or_ach_created_at,
            "transactionId" AS transaction_id,
            id AS cc_or_ach_transaction_id,
            "declineCode" AS cc_or_ach_error_code,
            amount * 0.01 AS transaction_amount_usd
        FROM stripe_transactions
    ),

    ach_transactions AS (
        SELECT 
            "createdAt" AT TIME ZONE 'PST' AS cc_or_ach_created_at,
            "transactionID" AS transaction_id,
            id AS cc_or_ach_transaction_id,
            "returnCode" AS cc_or_ach_error_code,
            "routingNumber" AS ach_transaction_routing_number,
            amount * 0.01 AS transaction_amount_usd,
            "achFileID" AS ach_file_id
        FROM ach_transactions
    )

    SELECT
        'Credit Card Transaction' AS transaction_type,
        cc_or_ach_created_at,
        transaction_id,
        cc_or_ach_transaction_id,
        cc_or_ach_error_code,
        transaction_amount_usd,
        NULL AS ach_file_id,
        NULL AS ach_transaction_routing_number,
        CASE WHEN cc_or_ach_error_code IS NULL THEN TRUE ELSE FALSE END AS cc_or_ach_transaction_succeeded
    FROM cc_transactions

    UNION ALL

    SELECT
        'ACH Transaction' AS transaction_type,
        cc_or_ach_created_at,
        transaction_id,
        cc_or_ach_transaction_id,
        cc_or_ach_error_code,
        transaction_amount_usd,
        ach_file_id,
        ach_transaction_routing_number,
        CASE WHEN cc_or_ach_error_code IS NULL THEN TRUE ELSE FALSE END AS cc_or_ach_transaction_succeeded
    FROM ach_transactions
),

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

payments_extended AS (
    SELECT 
        -- Payment Information
        p.payment_id,
        p.user_id,
        p.fund_id,
        f.fund_plan_manager,
        f.fund_lpoa_status,
        p.contribution_id,
        
        -- Payment Amounts
        p.payment_amount_usd,
        p.payment_processing_fee_usd,
        p.payment_subscription_fee_usd,
        p.payment_tip_usd,
        
        -- Payment Dates
        p.payment_created_at,
        p.payment_transaction_date,
        p.payment_due_date,
        p.payment_updated_at,
        p.payment_deleted_at,
        
        -- Payment Dates (UTC)
        p.payment_created_at_utc,
        p.payment_transaction_date_utc,
        p.payment_due_date_utc,
        p.payment_updated_at_utc,
        p.payment_deleted_at_utc,
        
        -- Payment ACH Dates
        p.payment_initiated_at,
        p.payment_pull_settled_at,
        p.payment_pushed_at,
        p.payment_push_settled_at,
        p.payment_settled_at,
        p.payment_refunded_at,
        p.payment_cancel_requested_at,
        p.payment_cancelled_at,
        
        -- Payment ACH Dates
        p.payment_initiated_at_utc,
        p.payment_pull_settled_at_utc,
        p.payment_pushed_at_utc,
        p.payment_push_settled_at_utc,
        p.payment_settled_at_utc,
        p.payment_refunded_at_utc,
        p.payment_cancel_requested_at_utc,
        p.payment_cancelled_at_utc,
        
        -- Payment Type
        p.payment_type,
        p.payment_method_type,
        p.payment_is_for,
        
        -- Payment ACH Info
        p.payment_bank_account_id,
        
        -- Payment Credit Card Info
        p.payment_credit_card_id,
        
        -- Payment Status
        p.payment_retries,
        p.payment_settlement_method,
        p.payment_state,
        
        -- Transaction Info
        t.transaction_id,
        t.transaction_state,
        t.transaction_type_detail,
        t.transaction_created_at,
        t.transaction_settled_at,
        
        -- CC or ACH Information
        cat.transaction_type AS cc_ach_transaction_type,
        cat.cc_or_ach_created_at,
        cat.cc_or_ach_transaction_id,
        cat.cc_or_ach_error_code,
        cat.transaction_amount_usd AS cc_or_ach_transaction_amount_usd,
        cat.ach_file_id,
        cat.ach_transaction_routing_number,
        cat.cc_or_ach_transaction_succeeded,
        
        -- Contribution Retention
        -- DEPRECATED
        -- CASE 
        --   WHEN NOT cat.cc_or_ach_transaction_succeeded OR cat.cc_or_ach_transaction_succeeded IS NULL THEN FALSE
        --   WHEN p.payment_state IN ('initiated', 'settled') AND cat.cc_or_ach_transaction_succeeded THEN TRUE
        --   ELSE FALSE
        -- END AS payment_success_final,
        
        -- CASE 
        --   WHEN p.payment_state IN ('initiated', 'settled') AND cat.cc_or_ach_transaction_succeeded THEN 'Success'
        --   WHEN cat.transaction_type = 'Credit Card Transaction' AND NOT cat.cc_or_ach_transaction_succeeded THEN 'Credit Card Error'
        --   WHEN cat.transaction_type = 'ACH Transaction' AND NOT cat.cc_or_ach_transaction_succeeded THEN 'ACH Return'
        --   WHEN NOT cat.cc_or_ach_transaction_succeeded THEN 'CC/ACH Transaction Failure'
        --   ELSE 'Other / Legacy Payment Failure'
        -- END AS payment_success_final_reason,
        -- /DEPRECATED
        
        -- Payment Category
        CASE 
            WHEN p.payment_type = 'voluntary-fee' THEN 'operational' 
            WHEN f.fund_plan_manager = 'cyp' THEN 'bill.com'
            WHEN f.fund_plan_manager = 'safe' OR f.fund_lpoa_status <> 'completed' OR f.fund_lpoa_status IS NULL THEN 'cma'
            WHEN f.fund_plan_manager = 'my529' OR f.fund_lpoa_status = 'completed' THEN 'my529'
            ELSE 'anomaly'
        END AS payment_category,
        
        CASE 
            WHEN 
            p.payment_type = 'voluntary-fee' 
            OR f.fund_plan_manager = 'cyp' 
            OR (f.fund_plan_manager = 'my529' OR f.fund_lpoa_status = 'completed')
            THEN NULL
            
            WHEN f.fund_plan_manager = 'safe' THEN 'cma-safe'
            
            WHEN f.fund_plan_manager = 'my529' AND (f.fund_lpoa_status <> 'completed' OR f.fund_lpoa_status IS NULL) THEN 'cma-my529'
        
            ELSE 'anomaly'
        END AS payment_category_cma_type,
        
        CASE
            -- Any payment is failed / returned / cancelled
            WHEN p.payment_state IN ('failed', 'returned') OR t.transaction_state IN ('failed', 'returned') THEN 'failed/returned'
            WHEN p.payment_state = 'refunded' OR t.transaction_state = 'refunded' THEN 'refunded'
            WHEN p.payment_state = 'cancelled' OR t.transaction_state = 'cancelled' THEN 'cancelled'
            
            -- Voluntary Fee Payments
            WHEN p.payment_type = 'voluntary-fee' AND p.payment_state IN ('settled', 'initiated') AND p.payment_credit_card_id IS NOT NULL THEN 'stripe completed'
            WHEN p.payment_type = 'voluntary-fee' AND p.payment_state IN ('settled', 'initiated') AND p.payment_bank_account_id IS NOT NULL THEN 'ach completed'
            
            -- Voluntary Fee Anomaly
            WHEN p.payment_type = 'voluntary-fee' THEN 'voluntary fee anomaly'
            
            -- Credit Card Payments --
            WHEN 
            p.payment_credit_card_id IS NOT NULL 
            
            -- F|T|F or T|F|T
            AND (
                (
                p.payment_pull_settled_at IS NULL
                AND p.payment_push_settled_at IS NOT NULL
                AND p.payment_settled_at IS NULL
                )
                OR (
                p.payment_pull_settled_at IS NOT NULL
                AND p.payment_push_settled_at IS NULL
                AND p.payment_settled_at IS NOT NULL
                )
            )
            THEN 'data error - stripe'
            
            WHEN p.payment_credit_card_id IS NOT NULL AND (p.payment_state = 'settled' OR t.transaction_state = 'settled') THEN 'stripe completed'
            WHEN p.payment_credit_card_id IS NOT NULL THEN 'stripe completed'
            
            -- ACH Payments --
            WHEN 
            p.payment_bank_account_id IS NOT NULL 
            
            -- F|F|F
            AND (
                (
                p.payment_pull_settled_at IS NULL
                AND p.payment_push_settled_at IS NULL
                AND p.payment_settled_at IS NULL
                )
            )
            
            AND p.payment_state <> 'settled' 
            AND t.transaction_state <> 'settled'
            THEN 'pull pending'
            
            WHEN 
            p.payment_bank_account_id IS NOT NULL 
            
            -- F|F|T or F|T|F or F|T|T
            AND (
                (
                p.payment_pull_settled_at IS NULL
                AND p.payment_push_settled_at IS NULL
                AND p.payment_settled_at IS NOT NULL
                )
                
                OR (
                p.payment_pull_settled_at IS NULL
                AND p.payment_push_settled_at IS NOT NULL
                -- payment_settled_at can be true or false
                )
            )
            THEN 'data error - ach'
            
            WHEN p.payment_bank_account_id IS NOT NULL THEN 'ach completed'
            
            WHEN p.payment_state = 'settled' OR t.transaction_state = 'settled' THEN 'ach completed'
            
            -- Catch-All
            ELSE 'data anomaly - unaccounted for'
        END AS payment_workflow_state,
        
        (
            p.payment_type = 'ach'
            AND (
            p.payment_state NOT IN ('cancelled', 'failed')
            OR (
                p.payment_state IN ('cancelled', 'failed')
                AND cat.cc_or_ach_error_code IS NOT NULL
            )
            )
        )
        OR (
            p.payment_type = 'stripe'
            AND p.payment_state NOT IN ('cancelled', 'failed')
            AND cat.cc_or_ach_error_code IS NULL
        ) AS payment_counts_cancel_issue
    
    FROM ayments_cleaned AS p
    LEFT JOIN (
        SELECT DISTINCT
            payment_id,
            FIRST_VALUE(transaction_id) OVER (PARTITION BY payment_id ORDER BY transaction_created_at DESC) AS transaction_id,
            FIRST_VALUE(transaction_created_at) OVER (PARTITION BY payment_id ORDER BY transaction_created_at DESC) AS transaction_created_at, 
            FIRST_VALUE(transaction_state) OVER (PARTITION BY payment_id ORDER BY transaction_created_at DESC) AS transaction_state,
            FIRST_VALUE(transaction_type_detail) OVER (PARTITION BY payment_id ORDER BY transaction_created_at DESC) AS transaction_type_detail, 
            FIRST_VALUE(transaction_settled_at) OVER (PARTITION BY payment_id ORDER BY transaction_created_at DESC) AS transaction_settled_at
        FROM transactions_cleaned
        WHERE 
            transaction_type_detail NOT IN('ach-push-tip', 'ach-push-fee')
    ) t
    ON p.payment_id = t.payment_id
    LEFT JOIN cc_ach_transactions AS cat
    ON t.transaction_id = cat.transaction_id
    LEFT JOIN funds_cleaned AS f
    ON p.fund_id = f.fund_id
),

users_cleaned AS (
    SELECT
        -- User PII
        u.id AS user_id,
        u.uuid AS user_uuid,
        u.email AS user_email,
        CONCAT(u."firstName", ' ', u."lastName") AS user_full_name,
        u."firstName" AS user_first_name,
        u."lastName" AS user_last_name,
        u."streetAddress" AS user_street_address,
        u."zipCode" AS user_zip_code,
        u.city AS user_city,
        u.state AS user_state,
        u.dob AS user_dob,
        u.photo AS user_photo,
        u."passwordHash" AS user_password_hash,
        AGE(NOW(), u.dob) AS user_age,
        EXTRACT(year FROM AGE(NOW(), u.dob)) AS user_age_years,
        EXTRACT(month FROM AGE(NOW(), u.dob)) AS user_age_months,
        u.gender AS user_gender,
        
        
        -- User Account Dates
        u."createdAt" AT TIME ZONE 'PST' AS user_created_at,
        u."updatedAt" AT TIME ZONE 'PST' AS user_updated_at,
        u."deletedAt" AT TIME ZONE 'PST' AS user_deleted_at,
        
        -- User Account Info
        u.admin OR u.id IN (57193, 545, 45080, 40000, 2000000) OR u.email ILIKE '%support@%' OR u.email ILIKE '%backer.com%' OR u.test AS user_is_admin,
        u.status AS user_status,
        u."statusReason" AS user_status_reason,
        u.status = 'active' OR u.status IS NULL AS user_is_active,
        
        -- Fee Info
        u.pricing AS user_pricing,
        u."voluntaryFee" * 0.01 AS user_voluntary_fee_usd,
        u."exemptFromFee" AS user_exempt_from_fee,
        u."bankAccountID" AS user_bank_account_id,
        u."creditCardID" AS user_credit_card_id,
        u."ccStripeCustomerID" AS user_cc_stripe_customer_id,
        
        -- User UTM Info
        CASE WHEN u."utmSource" = 'foundation' THEN TRUE ELSE FALSE END AS user_from_foundation,
        u."utmSource" AS user_utm_source,
        u."utmMedium" AS user_utm_medium,
        u."utmCampaign" AS user_utm_campaign,
        
        -- User Era Info
        CASE 
            WHEN "createdAt" AT TIME ZONE 'PST' < '2020-07-01' THEN 'legacy'
            WHEN "createdAt" AT TIME ZONE 'PST' < '2021-11-24' THEN 'subscription' -- place holder date for now
            ELSE 'backer2'
        END AS user_era,
        
        -- User Misc Info
        u."currentFundDraftId" AS user_current_fund_draft_id,
        u.variant AS user_variant
        FROM users u
),

-- AGGREGATE
voluntary_fee_per_user AS (
    SELECT
        u.user_id,
        p.payment_id,
        p.transaction_id,
        p.transaction_created_at,
        p.payment_created_at,
        p.payment_amount_usd,
        p.cc_or_ach_transaction_id,
        p.payment_workflow_state,
        DATE_TRUNC('month', p.payment_created_at) AS payment_created_at_month

    FROM users_cleaned AS u
    
    INNER JOIN payments_extended AS p
    ON u.user_id = p.user_id

    WHERE
        p.payment_category = 'operational'
),

voluntary_fee_retention_table AS (
    SELECT
        -- payment per owner this month
        user_id,
        payment_id,
        transaction_id,
        cc_or_ach_transaction_id,
        payment_workflow_state,
        payment_created_at_month AS current_payment_created_at_month,

        -- next expected payment
        DATE_TRUNC('month', payment_created_at + INTERVAL '1 month') AS next_expected_payment_created_at_month,

        -- payment per owner next month
        LEAD(payment_id, 1)
            OVER (
                PARTITION BY user_id
                ORDER BY payment_created_at_month
            )
        AS next_actual_payment_id,
        
        LEAD(transaction_id, 1)
            OVER (
                PARTITION BY user_id
                ORDER BY payment_created_at_month
            )
        AS next_actual_transaction_id,
        
        LEAD(cc_or_ach_transaction_id, 1)
            OVER (
                PARTITION BY user_id
                ORDER BY payment_created_at_month
            )
        AS next_actual_cc_or_ach_transaction_id,

        LEAD(payment_workflow_state, 1)
            OVER (
                PARTITION BY user_id
                ORDER BY payment_created_at_month
            )
        AS next_actual_payment_workflow_state,
        
        LEAD(payment_created_at_month, 1)
            OVER (
                PARTITION BY user_id
                ORDER BY payment_created_at_month
            )
        AS next_actual_payment_created_at_month

    FROM voluntary_fee_per_user
)

SELECT
    user_id,
    payment_id,
    transaction_id,
    cc_or_ach_transaction_id,
    payment_workflow_state,
    current_payment_created_at_month,

    -- Make sure resurrected users are counted when they are resurrected
    CASE
        -- Retained
        WHEN 
            -- current payment successful
            payment_workflow_state IN ('stripe completed', 'ach completed')
            
            -- next payment occurs the next month
            AND next_actual_payment_created_at_month = next_expected_payment_created_at_month
            
            -- next payment successful
            AND next_actual_payment_workflow_state IN ('stripe completed', 'ach completed')
        THEN next_expected_payment_created_at_month
        
        -- Resurrected
        WHEN
            -- next payment happens this month or after (but this month wasn't successful)
            next_actual_payment_created_at_month >= next_expected_payment_created_at_month
            
            -- next payment successful
            AND next_actual_payment_workflow_state IN ('stripe completed', 'ach completed')
        THEN next_actual_payment_created_at_month
        
        ELSE next_expected_payment_created_at_month
    END AS next_expected_payment_created_at_month,

    next_actual_payment_id,
    next_actual_transaction_id,
    next_actual_cc_or_ach_transaction_id,
    next_actual_payment_workflow_state,
    next_actual_payment_created_at_month,

    CASE
        -- Current Month
        WHEN
            DATE_TRUNC('month', NOW()) = current_payment_created_at_month
        THEN 'Current Month'

        -- Retained
        WHEN 
            -- current payment successful
            payment_workflow_state IN ('stripe completed', 'ach completed')
            
            -- next payment occurs next month
            AND next_actual_payment_created_at_month = next_expected_payment_created_at_month
            
            -- next payment successful
            AND next_actual_payment_workflow_state IN ('stripe completed', 'ach completed')
        THEN 'Retained'

        -- Resurrected
        WHEN
            -- next payment occurs next month (but this month wasn't successful)
            next_actual_payment_created_at_month = next_expected_payment_created_at_month
            
            -- next payment successful
            AND next_actual_payment_workflow_state IN ('stripe completed', 'ach completed')
        THEN 'Resurrected'

        -- Stale
        WHEN
            -- current payment unsuccessful
            payment_workflow_state IN ('cancelled', 'returned', 'refunded')
            
            -- next payment occurs next month or no next payment made
            AND (
              next_actual_payment_created_at_month = next_expected_payment_created_at_month
              OR next_actual_payment_created_at_month IS NULL
            )
            
            -- next payment unsuccessful or no next payment
            AND (
                next_actual_payment_workflow_state IN ('cancelled', 'returned', 'refunded')
                OR next_actual_payment_workflow_state IS NULL
            )
        THEN 'Stale'

        -- Involuntary Churn
        WHEN
            -- current payment successful
            payment_workflow_state IN ('stripe completed', 'ach completed')
            
            -- actual payment date happens
            AND next_actual_payment_created_at_month = next_expected_payment_created_at_month
            
            -- next payment cancelled or transaction returned error
            AND next_actual_payment_workflow_state IN ('cancelled', 'returned', 'refunded')
        THEN 'Involuntary Churn'

        -- Voluntary Churn
        WHEN 
            -- Final Voluntary Churn
            (
                -- current payment successful
                payment_workflow_state IN ('stripe completed', 'ach completed')
                
                -- no next payment
                AND next_actual_payment_created_at_month IS NULL
            )
            
            OR
            
            -- Resurrected Voluntary Churn
            (
                -- current payment successful
                payment_workflow_state IN ('stripe completed', 'ach completed')
                
                -- next payment date occurs after next month
                AND next_actual_payment_created_at_month > next_expected_payment_created_at_month
            )
        THEN 'Voluntary Churn'

        -- Anomalies
        ELSE 'Anomaly'
    END AS churn_status

FROM voluntary_fee_retention_table