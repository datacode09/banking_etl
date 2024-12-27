# banking_etl

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
