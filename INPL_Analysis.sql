WITH 

-- CLEAN DATA SOURCES
contributions_cleaned AS (
    SELECT 
    -- Contributor PII
    c.id AS contributor_id,
    c."userID" AS contributor_user_id,
    
    -- Contributor Dates
    c."createdAt" AT TIME ZONE 'PST' AS contributor_created_at,
    c."updatedAt" AT TIME ZONE 'PST' AS contributor_updated_at,
    c."deletedAt" AT TIME ZONE 'PST' AS contributor_deleted_at,
    
    -- Contributor Info
    c.type AS contributor_type,
    c.relationship AS contributor_relationship,
    
    -- Contribution Dates
    cc."createdAt" AT TIME ZONE 'PST' AS contribution_created_at,
    cc."updatedAt" AT TIME ZONE 'PST' AS contribution_updated_at,
    cc."deletedAt" AT TIME ZONE 'PST' AS contribution_deleted_at,
    
    -- Fund Info
    c."fundID" AS fund_id,
    
    -- Contribution Info
    cc.id AS contribution_id,
    cc."originalContributionId" AS contribution_original_id,
    cc.state AS contribution_state,
    cc.frequency AS contribution_frequency,
    cc.variant AS contribution_variant,
    cc."createdAsStarterGift" AS contribution_created_at_starter_gift,
    
    -- Contribution Activity Info
    cc.state NOT IN ('cancelled', 'invalid', 'edited') AS contribution_is_active,
    cc.frequency <> 'onetime' AS contribution_is_recurring,
    
    -- Contribution Amount Info
    cc.amount * 0.01 AS contribution_amount_usd,
    cc.tip * 0.01 AS contribution_tip_usd,
    cc."processingFee" * 0.01 AS contribution_processing_fee_usd,
    
    -- Contribution Payment Info
    cc."bankAccountID" AS contribution_bank_account_id,
    cc."creditCardID" AS contribution_credit_card_id,
    cc."stripeSubscriptionID" AS contribution_stripe_subscription_id,
    
    -- Contribution Recipient Info
    cc."recipientName" AS contribution_recipient_name,
    cc."recipientParentName" AS contribution_recipient_parent_name,
    cc."recipientMessage" AS contribution_recipient_message,
    cc."recipientEmailAddress" AS contribution_recipient_email
    
    FROM contributors c
    LEFT JOIN contributions cc
    ON c."userID" = cc."userID"
        AND c."fundID" = cc."collegeFundID"
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
boost_payments AS (
    SELECT DISTINCT
        p.payment_created_at AS boost_payment_created_at,
        p.contribution_id,
        p.fund_id,
        f.fund_owner_user_id,
        p.payment_initiated_at AS payment_redeemed_at,
        CASE 
            WHEN payment_deleted_at IS NOT NULL THEN 'Clawed'
            WHEN payment_initiated_at IS NULL THEN 'Unconfirmed'
            WHEN payment_initiated_at IS NOT NULL THEN 'Redeemed'
            WHEN payment_pushed_at IS NOT NULL THEN 'Settled'
            ELSE 'Anomaly'
        END AS boost_payment_state
    FROM payments_cleaned AS p
    LEFT JOIN funds_cleaned AS f
    ON p.fund_id = f.fund_id
    WHERE
        p.payment_type = 'boost'
        AND p.payment_amount_usd = 529
),

fund_conversion_rates AS (
    SELECT 
        f.fund_owner_user_id,
        COUNT(DISTINCT f.fund_id) > 0 AS has_funds,
        COUNT(DISTINCT 
            CASE
            WHEN 
                f.fund_plan_manager = 'my529' 
                AND f.fund_account_number IS NOT NULL 
                AND f.fund_lpoa_status = 'completed'
            THEN f.fund_id
            END
        ) > 0 AS has_my529_funds
    FROM funds_cleaned AS f
    GROUP BY 1
),

contribution_conversion_rates AS (
    SELECT 
        contributor_user_id,
        COUNT(DISTINCT contribution_id) AS num_contributions,
        COUNT(DISTINCT CASE WHEN contribution_is_recurring THEN contribution_id END) AS num_contributions_recurring,
        COUNT(DISTINCT CASE WHEN NOT contribution_is_recurring THEN contribution_id END) AS num_contributions_onetime,
        SUM(CASE WHEN contribution_is_recurring THEN contribution_amount_usd END) AS avg_recurring_contribution_amount_usd,
        SUM(CASE WHEN NOT contribution_is_recurring THEN contribution_amount_usd END) AS avg_onetime_contribution_amount_usd
    FROM contributions_cleaned
    WHERE
        contribution_id IS NOT NULL
        AND contribution_state NOT IN ('cancelled', 'edited')
        AND contributor_type = 'owner'
    GROUP BY 1
)

SELECT 
    u.user_created_at,
    u.user_id,
    u.user_email,
    bp.fund_id IS NOT NULL AS fund_has_boost,
    bp.boost_payment_created_at,
    bp.boost_payment_state,
    COALESCE(f.has_funds, FALSE) AS has_funds,
    COALESCE(f.has_my529_funds, FALSE) AS has_my529_funds,
    COALESCE(c.num_contributions, 0) AS num_contributions,
    COALESCE(c.num_contributions_recurring, 0) AS num_contributions_recurring,
    COALESCE(c.num_contributions_onetime, 0) AS num_contributions_onetime,
    COALESCE(c.num_contributions, 0) > 0 AS has_contributions,
    COALESCE(c.num_contributions_recurring, 0) > 0 AS has_contributions_recurring,
    COALESCE(c.num_contributions_onetime, 0) > 0 AS has_contributions_onetime,
    COALESCE(c.avg_recurring_contribution_amount_usd, 0) AS avg_recurring_contribution_amount_usd,
    COALESCE(c.avg_onetime_contribution_amount_usd, 0) AS avg_onetime_contribution_amount_usd
FROM users_cleaned AS u

-- All users that got a boost
LEFT JOIN boost_payments bp
ON bp.fund_owner_user_id = u.user_id

-- Fund creation conversion rate + my529 upgrade
LEFT JOIN fund_conversion_rates AS f
ON f.fund_owner_user_id = u.user_id

-- Contribution conversion rate
LEFT JOIN contribution_conversion_rates AS c
ON c.contributor_user_id = u.user_id

WHERE
    u.user_is_active
    AND NOT u.user_is_admin