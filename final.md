### **Linkage Document**

This linkage document describes the mapping of the source data schema to the target data schema and the logic used in the PySpark code to calculate each field.

---

### **Source Schema**

| **Field Name**                 | **Description**                                                                                 |
|--------------------------------|-------------------------------------------------------------------------------------------------|
| `uen`                          | Unique borrower identifier.                                                                     |
| `rpt_prd_end_date`             | Reporting period end date (YYYY-MM-DD).                                                        |
| `prod_nm`                      | Product name associated with the borrower.                                                     |
| `level_1_product_description`  | High-level product category (e.g., "deposit", "loan").                                         |
| `balance`                      | Balance amount for the specific product.                                                       |
| `cad_to_us_exchange_rate`      | Exchange rate to convert CAD balances to USD.                                                  |

---

### **Target Schema**

| **Field Name**         | **Description**                                                                                          | **Source Fields Used**                        | **Logic**                                                                                       |
|-------------------------|----------------------------------------------------------------------------------------------------------|-----------------------------------------------|-------------------------------------------------------------------------------------------------|
| `uen`                  | Unique borrower identifier.                                                                              | `uen`                                         | Direct mapping.                                                                                |
| `rpt_prd_end_date`     | Reporting period end date.                                                                               | `rpt_prd_end_date`                            | Direct mapping.                                                                                |
| `dep_bal_curr_usd`     | Current deposit balance in USD.                                                                          | `balance`, `cad_to_us_exchange_rate`          | Sum of `balance` for deposits in the current year, converted to USD using `cad_to_us_exchange_rate`. |
| `dep_bal_curr_cad`     | Current deposit balance in CAD.                                                                          | `balance`                                     | Sum of `balance` for deposits in the current year.                                             |
| `dep_bal_prev_usd`     | Previous year's deposit balance in USD.                                                                 | `balance`, `cad_to_us_exchange_rate`          | Sum of `balance` for deposits from the previous year, converted to USD using `cad_to_us_exchange_rate`. |
| `dep_bal_prev_cad`     | Previous year's deposit balance in CAD.                                                                 | `balance`                                     | Sum of `balance` for deposits from the previous year.                                          |
| `dep_bal_yoy`          | Year-over-year change in deposit balances.                                                              | `dep_bal_curr_usd`, `dep_bal_prev_usd`        | \((\text{dep\_bal\_curr\_usd} - \text{dep\_bal\_prev\_usd}) / \text{dep\_bal\_prev\_usd}\).                                            |
| `crd_bal_curr_usd`     | Current credit balance in USD.                                                                           | `balance`, `cad_to_us_exchange_rate`          | Sum of `balance` for loans in the current year, converted to USD using `cad_to_us_exchange_rate`. |
| `crd_bal_curr_cad`     | Current credit balance in CAD.                                                                           | `balance`                                     | Sum of `balance` for loans in the current year.                                                |
| `crd_bal_prev_usd`     | Previous year's credit balance in USD.                                                                  | `balance`, `cad_to_us_exchange_rate`          | Sum of `balance` for loans from the previous year, converted to USD using `cad_to_us_exchange_rate`. |
| `crd_bal_prev_cad`     | Previous year's credit balance in CAD.                                                                  | `balance`                                     | Sum of `balance` for loans from the previous year.                                             |
| `crd_bal_yoy`          | Year-over-year change in credit balances.                                                               | `crd_bal_curr_usd`, `crd_bal_prev_usd`        | \((\text{crd\_bal\_curr\_usd} - \text{crd\_bal\_prev\_usd}) / \text{crd\_bal\_prev\_usd}\).                                             |
| `prd_curr`             | List of current active products.                                                                        | `prod_nm`, `rpt_prd_end_date`                 | List of `prod_nm` for the current reporting period.                                             |
| `prd_prev`             | List of products from the previous year.                                                                | `prod_nm`, `rpt_prd_end_date`                 | List of `prod_nm` for the same reporting period 12 months prior.                                |
| `prd_penetration`      | Difference in the count of products in the current period divided by the count in the previous period.   | `prd_curr`, `prd_prev`                        | \((\text{len(prd\_curr)} - \text{len(prd\_prev)}) / \text{len(prd\_prev)}\).                                                         |

---

### **Linkage Details**

#### **1. Aggregation and Filtering**
- **Source Columns**: `balance`, `cad_to_us_exchange_rate`, `level_1_product_description`, `rpt_prd_end_date`.
- **Logic**:
  - Group by `uen` and `rpt_prd_end_date`.
  - Filter `level_1_product_description` for deposits or loans.
  - Separate calculations for current and previous periods.

#### **2. Currency Conversion**
- **Source Columns**: `balance`, `cad_to_us_exchange_rate`.
- **Logic**:
  - For USD balances, multiply `balance` by `cad_to_us_exchange_rate`.

#### **3. Year-over-Year Change**
- **Source Columns**: Aggregated values for deposits and loans.
- **Logic**:
  - Calculate the percentage change between the current and previous period balances.

#### **4. Product Lists**
- **Source Columns**: `prod_nm`, `rpt_prd_end_date`.
- **Logic**:
  - Use `collect_set` to group product names by `uen` for the current and previous periods.

#### **5. Product Penetration**
- **Source Columns**: `prd_curr`, `prd_prev`.
- **Logic**:
  - Calculate the relative difference in the number of products.

---

### **Key Considerations**
1. **Edge Cases**:
   - Handle scenarios where the borrower has no data for the current or previous period.
   - Avoid division by zero in YoY change and product penetration calculations.

2. **Performance Optimization**:
   - Use partitioning by `uen` and `rpt_prd_end_date` to minimize shuffling during aggregation.
   - Perform filtering early to reduce the size of intermediate datasets.

3. **Flexibility**:
   - The logic is adaptable for other currencies or additional product categories if required.

4. **Validation**:
   - Ensure consistency of input data types (e.g., date fields, numeric fields).

---

Let me know if further details are needed!
