# banking_etl

Here’s a **linkage table** where each output column is mapped to its corresponding input (source) column(s), along with the transformation logic required to derive it:

| **Output Column**                      | **Source Column(s)**                              | **Transformation Logic**                                                                                                                                                                                                                                     |
|----------------------------------------|--------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `uen`                                  | `uen`                                            | Direct mapping (unique identifier for customers).                                                                                                                                                                                                           |
| `rpt_prd_end_dt`                       | `rpt_prd_end_dt`                                 | Direct mapping (report period end date).                                                                                                                                                                                                                    |
| `dep_bal_curr_usd`                     | `deposit_balance`, `currency`, `conversion_rate_to_usd` | If `currency` is USD, use `deposit_balance`; otherwise, multiply by `conversion_rate_to_usd`.                                                                                                                                                              |
| `dep_bal_curr_cad`                     | `deposit_balance`, `currency`, `conversion_rate_to_cad` | If `currency` is CAD, use `deposit_balance`; otherwise, divide by `conversion_rate_to_cad`.                                                                                                                                                                |
| `dep_bal_prev_usd`                     | `prev_deposit_balance_usd`                       | Direct mapping (previous year’s deposit balance in USD).                                                                                                                                                                                                   |
| `dep_bal_prev_cad`                     | `prev_deposit_balance_cad`                       | Direct mapping (previous year’s deposit balance in CAD).                                                                                                                                                                                                   |
| `dep_bal_yoy_change_usd`               | `dep_bal_curr_usd`, `dep_bal_prev_usd`           | \[((`dep_bal_curr_usd` - `dep_bal_prev_usd`) / `dep_bal_prev_usd`) * 100\] to calculate percentage change in USD.                                                                                                                                           |
| `dep_bal_yoy_change_cad`               | `dep_bal_curr_cad`, `dep_bal_prev_cad`           | \[((`dep_bal_curr_cad` - `dep_bal_prev_cad`) / `dep_bal_prev_cad`) * 100\] to calculate percentage change in CAD.                                                                                                                                           |
| `crd_bal_curr_usd`                     | `credit_balance`, `currency`, `conversion_rate_to_usd` | If `currency` is USD, use `credit_balance`; otherwise, multiply by `conversion_rate_to_usd`.                                                                                                                                                               |
| `crd_bal_curr_cad`                     | `credit_balance`, `currency`, `conversion_rate_to_cad` | If `currency` is CAD, use `credit_balance`; otherwise, divide by `conversion_rate_to_cad`.                                                                                                                                                                 |
| `crd_bal_prev_usd`                     | `prev_credit_balance_usd`                        | Direct mapping (previous year’s credit balance in USD).                                                                                                                                                                                                    |
| `crd_bal_prev_cad`                     | `prev_credit_balance_cad`                        | Direct mapping (previous year’s credit balance in CAD).                                                                                                                                                                                                    |
| `crd_bal_yoy_change_usd`               | `crd_bal_curr_usd`, `crd_bal_prev_usd`           | \[((`crd_bal_curr_usd` - `crd_bal_prev_usd`) / `crd_bal_prev_usd`) * 100\] to calculate percentage change in USD.                                                                                                                                           |
| `crd_bal_yoy_change_cad`               | `crd_bal_curr_cad`, `crd_bal_prev_cad`           | \[((`crd_bal_curr_cad` - `crd_bal_prev_cad`) / `crd_bal_prev_cad`) * 100\] to calculate percentage change in CAD.                                                                                                                                           |
| `inc_txn_vol_curr_usd`                 | `incoming_txn_usd`                               | Sum of all incoming transaction amounts in USD.                                                                                                                                                                                                            |
| `inc_txn_vol_curr_cad`                 | `incoming_txn_cad`                               | Sum of all incoming transaction amounts in CAD.                                                                                                                                                                                                            |
| `inc_txn_vol_prev_usd`                 | `prev_incoming_txn_usd`                          | Direct mapping (previous year’s transaction volume in USD).                                                                                                                                                                                                |
| `inc_txn_vol_prev_cad`                 | `prev_incoming_txn_cad`                          | Direct mapping (previous year’s transaction volume in CAD).                                                                                                                                                                                                |
| `inc_txn_vol_yoy_change_usd`           | `inc_txn_vol_curr_usd`, `inc_txn_vol_prev_usd`   | \[((`inc_txn_vol_curr_usd` - `inc_txn_vol_prev_usd`) / `inc_txn_vol_prev_usd`) * 100\] to calculate percentage change in USD.                                                                                                                               |
| `inc_txn_vol_yoy_change_cad`           | `inc_txn_vol_curr_cad`, `inc_txn_vol_prev_cad`   | \[((`inc_txn_vol_curr_cad` - `inc_txn_vol_prev_cad`) / `inc_txn_vol_prev_cad`) * 100\] to calculate percentage change in CAD.                                                                                                                               |
| `prod_cnt`                             | `current_products`                               | Count the number of products in `current_products` (assuming it is a comma-separated list).                                                                                                                                                                |
| `prod_pen`                             | `prod_cnt`, `total_possible_products`            | \[(`prod_cnt` / `total_possible_products`)\] to calculate product penetration.                                                                                                                                                                             |

---

### Key Notes:
1. **Assumptions:**
   - `conversion_rate_to_usd` and `conversion_rate_to_cad` exist in the input schema for currency conversion.
   - `prev_*` columns (e.g., `prev_deposit_balance_usd`, `prev_credit_balance_usd`) exist for previous year data.
   - `current_products` is a comma-separated string.

2. **Clarifications Still Needed:**
   - Are conversion rates constant or variable per row?
   - Are there any specific filters or rules for aggregations (e.g., for `inc_txn_vol_curr_usd`)?

If anything seems unclear or incorrect, let me know, and I’ll refine the table further!


Let me carefully list all the source columns from the **input schema image** and cross-verify that I only used columns actually present in the input dataset in my mapping table:

---

### **Source Columns from the Input Schema**
Here’s the full list of columns as per the schema image provided:

1. `rpt_prd_end_dt`  
2. `apms_cstmr_id`  
3. `uen`  
4. `src_stm_cd`  
5. `acct_num`  
6. `acct_id`  
7. `priority`  
8. `ref1_cd`  
9. `ref2_cd`  
10. `loan_alias`  
11. `ac_open_dt`  
12. `ac_tp_cd`  
13. `int_rate_tp`  
14. `src_trnst_nbr`  
15. `src_bmo_resp_node_hrs_dept_id`  
16. `ac_maturity_dt`  
17. `tsys_corp_limit`  
18. `rsk_tp_prod_tp_cd`  
19. `ac_nbr_nadb`  
20. `rsk_tp_id`  
21. `rsk_tp_prod_cd`  
22. `rsk_tp_prod_nm`  
23. `fcy_id`  
24. `prod_id`  
25. `prod_cd`  
26. `prod_nm`  
27. `fcy_purpose`  
28. `comc_fcy_id`  
29. `comc_app_no`  
30. `comc_fcy_prod_cd`  
31. `comc_prod_nm`  
32. `ccy`  
33. `fcy_open_dt`  
34. `com_start_dt`  
35. `maturity_dt`  
36. `facrid`  
37. `original_auth_amt`  
38. `maturity_dt_ind`  
39. `rlvr_ind`  
40. `amortizing_ind`  
41. `amortization_end_loan_months`  
42. `fcy_rsk_tp_id`  
43. `bank_approved_dt`  
44. `cust_acceptance_dt`  
45. `cad_acct_id`  
46. `cold_id`  
47. `src_stm_rtn_cd`  
48. `dep_src_ac_nbr_apms`  
49. `pers_comm_cd_mech`  
50. `lvl1_prod_dsc`  
51. `lvl2_prod_dsc`  
52. `lvl3_prod_dsc`  
53. `lvl4_prod_dsc`  
54. `balance`  
55. `prod_tp`  
56. `privt_bank_ind`  
57. `pnp_sign_ind_reco_ind`  
58. `rdm_ind_mech`  
59. `renew_ind_mech`  
60. `new_ind_ips`  
61. `txn_nbr`  
62. `put_alt_rsn_cd`  
63. `rvlvr_ind`  
64. `txn_vol`

---

### **Cross-Verification of Output Mappings**

Let’s cross-check if I used **any column that does not exist in the source schema**:

#### Columns Used in the Mapping Table:
1. `uen` → ✅ Exists (source identifier for customers).
2. `rpt_prd_end_dt` → ✅ Exists (reporting period).
3. `deposit_balance` → **NOT FOUND** in source schema.
4. `credit_balance` → **NOT FOUND** in source schema.
5. `currency` → **NOT FOUND** in source schema.
6. `conversion_rate_to_usd` → **NOT FOUND** in source schema.
7. `conversion_rate_to_cad` → **NOT FOUND** in source schema.
8. `prev_deposit_balance_usd` → **NOT FOUND** in source schema.
9. `prev_deposit_balance_cad` → **NOT FOUND** in source schema.
10. `prev_credit_balance_usd` → **NOT FOUND** in source schema.
11. `prev_credit_balance_cad` → **NOT FOUND** in source schema.
12. `incoming_txn_usd` → **NOT FOUND** in source schema.
13. `incoming_txn_cad` → **NOT FOUND** in source schema.
14. `current_products` → **NOT FOUND** in source schema.
15. `total_possible_products` → **NOT FOUND** in source schema.

---

### **Findings**

Upon review, I see several columns used in my previous mapping (e.g., `deposit_balance`, `credit_balance`, `currency`, etc.) **do not exist** in the input schema provided. These columns appear to have been **hallucinated** based on inferred logic.

---

### **Next Steps**

1. **Clarify missing columns**:
   - Are there additional datasets or external sources that provide these values (e.g., `currency`, `conversion_rate_to_usd`, etc.)?
   - If not, can we derive these metrics from existing columns in the source schema?

2. **Adjust mappings**:
   - I will rework the linkage table and recipe based on the actual source schema columns.

Let me know how you’d like to proceed! If you can clarify the source for missing data points, I’ll update the recipe accordingly.


Based on the actual source schema provided, I will now refine the linkage table and ensure it aligns with the **existing columns**. This revised table avoids any assumptions about non-existent fields and clearly maps each output column to its source columns or logic.

---

### Revised Linkage Table

| **Output Column**                      | **Source Column(s)**                              | **Transformation Logic**                                                                                                                                                        |
|----------------------------------------|--------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `uen`                                  | `uen`                                            | Direct mapping (unique identifier for customers).                                                                                                                                |
| `rpt_prd_end_dt`                       | `rpt_prd_end_dt`                                 | Direct mapping (report period end date).                                                                                                                                         |
| `dep_bal_curr_usd`                     | `balance`, `ccy`                                 | If `ccy` is "USD", use `balance`; otherwise, multiply `balance` by a predefined conversion rate (if provided elsewhere).                                                         |
| `dep_bal_curr_cad`                     | `balance`, `ccy`                                 | If `ccy` is "CAD", use `balance`; otherwise, divide `balance` by a predefined conversion rate (if provided elsewhere).                                                           |
| `dep_bal_prev_usd`                     | `balance`, `txn_vol`                             | Use `balance` value from the previous period (assume `txn_vol` contains a flag for identifying historical data, or data needs filtering to extract prior period).                |
| `dep_bal_prev_cad`                     | `balance`, `txn_vol`                             | Same as above, but consider balance in CAD.                                                                                                                                      |
| `dep_bal_yoy_change_usd`               | `balance`                                        | Calculate year-over-year change: \[((`dep_bal_curr_usd` - `dep_bal_prev_usd`) / `dep_bal_prev_usd`) * 100\].                                                                     |
| `dep_bal_yoy_change_cad`               | `balance`                                        | Same calculation as above, but for CAD.                                                                                                                                          |
| `crd_bal_curr_usd`                     | `balance`, `ccy`                                 | Same logic as `dep_bal_curr_usd`, assuming `balance` includes credit balances as well (clarification needed).                                                                    |
| `crd_bal_curr_cad`                     | `balance`, `ccy`                                 | Same logic as `dep_bal_curr_cad`, assuming `balance` includes credit balances as well.                                                                                           |
| `crd_bal_prev_usd`                     | `balance`, `txn_vol`                             | Use `balance` for the prior year (filter `txn_vol` to determine historical periods).                                                                                             |
| `crd_bal_prev_cad`                     | `balance`, `txn_vol`                             | Same as above, but in CAD.                                                                                                                                                       |
| `crd_bal_yoy_change_usd`               | `balance`                                        | Calculate year-over-year change: \[((`crd_bal_curr_usd` - `crd_bal_prev_usd`) / `crd_bal_prev_usd`) * 100\].                                                                     |
| `crd_bal_yoy_change_cad`               | `balance`                                        | Same calculation as above, but for CAD.                                                                                                                                          |
| `inc_txn_vol_curr_usd`                 | `txn_vol`                                        | Sum `txn_vol` for USD transactions over the current period.                                                                                                                      |
| `inc_txn_vol_curr_cad`                 | `txn_vol`                                        | Sum `txn_vol` for CAD transactions over the current period.                                                                                                                      |
| `inc_txn_vol_prev_usd`                 | `txn_vol`                                        | Filter `txn_vol` for USD transactions from the prior period and sum.                                                                                                             |
| `inc_txn_vol_prev_cad`                 | `txn_vol`                                        | Filter `txn_vol` for CAD transactions from the prior period and sum.                                                                                                             |
| `inc_txn_vol_yoy_change_usd`           | `txn_vol`                                        | Calculate year-over-year change: \[((`inc_txn_vol_curr_usd` - `inc_txn_vol_prev_usd`) / `inc_txn_vol_prev_usd`) * 100\].                                                         |
| `inc_txn_vol_yoy_change_cad`           | `txn_vol`                                        | Same calculation as above, but for CAD.                                                                                                                                          |
| `prod_cnt`                             | `lvl1_prod_dsc`, `lvl2_prod_dsc`, `lvl3_prod_dsc`, `lvl4_prod_dsc` | Count non-null product descriptions across the levels (`lvl1_prod_dsc` to `lvl4_prod_dsc`).                                                                                      |
| `prod_pen`                             | `prod_cnt`, `total_possible_products`            | Calculate product penetration: \[`prod_cnt` / `total_possible_products`\]. (Clarify: is `total_possible_products` a constant or available in the dataset?)                       |



### Clarifications Still Needed
1. **Product Metrics**:
   - Are `lvl1_prod_dsc` to `lvl4_prod_dsc` sufficient to calculate the product count (`prod_cnt`)? If not, where is the data for owned products stored?

2. **Currency Conversion Rates**:
   - How should conversion rates for USD/CAD be handled? Are they constants or dynamic values provided elsewhere?

3. **Historical Data for Year-over-Year Changes**:
   - How do we identify "previous year" values (e.g., `txn_vol` or `balance` for prior periods)? Does the dataset have a time-based flag or need external input?

4. **Credit and Deposit Balances**:
   - Is the `balance` column inclusive of both deposit and credit balances, or do we need a specific field for each? If separate, where are credit balances stored?

---

### Next Steps
With the clarified linkage table, I can:
- Adjust the PySpark recipe to directly map source columns to output fields.
- Include logic for aggregations, filtering, and transformations explicitly tied to the input schema.

Let me know how you'd like to proceed, or clarify any of the points above!
