WITH 
-- Filter deposits and calculate balances for current year
deposit_current AS (
    SELECT 
        uen,
        rpt_prd_end_date,
        SUM(balance * cad_to_us_exchange_rate) AS dep_bal_curr_usd,
        SUM(balance) AS dep_bal_curr_cad
    FROM source_table
    WHERE level_1_product_description = 'deposit'
      AND YEAR(rpt_prd_end_date) = YEAR(CURRENT_DATE)
    GROUP BY uen, rpt_prd_end_date
),

-- Filter deposits and calculate balances for previous year
deposit_prev AS (
    SELECT 
        uen,
        SUM(balance * cad_to_us_exchange_rate) AS dep_bal_prev_usd,
        SUM(balance) AS dep_bal_prev_cad
    FROM source_table
    WHERE level_1_product_description = 'deposit'
      AND YEAR(rpt_prd_end_date) = YEAR(CURRENT_DATE) - 1
    GROUP BY uen
),

-- Filter loans and calculate balances for current year
loan_current AS (
    SELECT 
        uen,
        rpt_prd_end_date,
        SUM(balance * cad_to_us_exchange_rate) AS crd_bal_curr_usd,
        SUM(balance) AS crd_bal_curr_cad
    FROM source_table
    WHERE level_1_product_description = 'loan'
      AND YEAR(rpt_prd_end_date) = YEAR(CURRENT_DATE)
    GROUP BY uen, rpt_prd_end_date
),

-- Filter loans and calculate balances for previous year
loan_prev AS (
    SELECT 
        uen,
        SUM(balance * cad_to_us_exchange_rate) AS crd_bal_prev_usd,
        SUM(balance) AS crd_bal_prev_cad
    FROM source_table
    WHERE level_1_product_description = 'loan'
      AND YEAR(rpt_prd_end_date) = YEAR(CURRENT_DATE) - 1
    GROUP BY uen
),

-- Aggregate product lists for current and previous year
product_lists AS (
    SELECT 
        uen,
        ARRAY_AGG(CASE WHEN YEAR(rpt_prd_end_date) = YEAR(CURRENT_DATE) THEN prod_nm END) AS prd_curr,
        ARRAY_AGG(CASE WHEN YEAR(rpt_prd_end_date) = YEAR(CURRENT_DATE) - 1 THEN prod_nm END) AS prd_prev
    FROM source_table
    GROUP BY uen
)

-- Combine all calculations into the final result
SELECT 
    dc.uen,
    dc.rpt_prd_end_date,
    dc.dep_bal_curr_usd,
    dc.dep_bal_curr_cad,
    dp.dep_bal_prev_usd,
    dp.dep_bal_prev_cad,
    (dc.dep_bal_curr_usd - dp.dep_bal_prev_usd) / NULLIF(dp.dep_bal_prev_usd, 0) AS dep_bal_yoy,
    lc.crd_bal_curr_usd,
    lc.crd_bal_curr_cad,
    lp.crd_bal_prev_usd,
    lp.crd_bal_prev_cad,
    (lc.crd_bal_curr_usd - lp.crd_bal_prev_usd) / NULLIF(lp.crd_bal_prev_usd, 0) AS crd_bal_yoy,
    pl.prd_curr,
    pl.prd_prev,
    (CARDINALITY(pl.prd_curr) - CARDINALITY(pl.prd_prev)) / NULLIF(CARDINALITY(pl.prd_prev), 0) AS prd_penetration
FROM 
    deposit_current dc
LEFT JOIN deposit_prev dp ON dc.uen = dp.uen
LEFT JOIN loan_current lc ON dc.uen = lc.uen AND dc.rpt_prd_end_date = lc.rpt_prd_end_date
LEFT JOIN loan_prev lp ON dc.uen = lp.uen
LEFT JOIN product_lists pl ON dc.uen = pl.uen;
