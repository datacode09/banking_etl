-- deposit metric validation

SELECT 
    uen, 
    SUM(balance) AS agg_balance, 
    rpt_prd_end_dt
FROM 
    nacb_universe_account
WHERE 
    uen = '37203660' 
    AND to_date(rpt_prd_end_dt, 'YYYY-MM-DD') = '2023-09-'
    AND ac_tp_cd IN ('D', 'T')
GROUP BY 
    uen, rpt_prd_end_dt

UNION

SELECT 
    uen, 
    SUM(balance) AS agg_balance, 
    rpt_prd_end_dt
FROM 
    nacb_universe_account
WHERE 
    uen = '37203660' 
    AND to_date(rpt_prd_end_dt, 'YYYY-MM-DD') = '2023-09'
    AND ac_tp_cd IN ('D', 'T')
GROUP BY 
    uen, rpt_prd_end_dt;

-- credit metric validation

SELECT uen, balance_new_1, rpt_prd_end_dt 
FROM aim_features_term_loan 
WHERE uen = '37530374' 
AND to_date(rpt_prd_end_dt, 'YYYY-MM-DD') = '2023-09-30'

UNION

SELECT uen, balance_new_1, rpt_prd_end_dt 
FROM aim_features_term_loan 
WHERE uen = '37530374' 
AND to_date(rpt_prd_end_dt, 'YYYY-MM-DD') = '2024-09-30';


-- transaction metric validation

SELECT 
    uen,
    sum_credit_trans_ext1,
    rpt_prd_end_dt,
    SUM(sum_credit_trans_ext1) OVER (
        PARTITION BY uen
        ORDER BY rpt_prd_end_dt
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_sum
FROM 
    aim_features_op_acct_txn
WHERE 
    uen = '37203660'
    AND to_date(rpt_prd_end_dt, 'YYYY-MM-DD') >= '2023-09-30'
    AND to_date(rpt_prd_end_dt, 'YYYY-MM-DD') <= '2024-09-30'
ORDER BY 
    rpt_prd_end_dt;

-- product metric validation
SELECT uen, lvl3_prod_dsc, rpt_prd_end_dt 
FROM nacb_universe_account 
WHERE uen = '37203660' 
AND to_date(rpt_prd_end_dt, 'YYYY-MM-DD') = '2023-09-30'

UNION

SELECT uen, lvl3_prod_dsc, rpt_prd_end_dt 
FROM nacb_universe_account 
WHERE uen = '37203660' 
AND to_date(rpt_prd_end_dt, 'YYYY-MM-DD') = '2024-09-30';





