from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Load the source data
input_df = dataiku.spark.get_dataframe("source_dataset")

# Convert `rpt_prd_end_dt` to a proper date format
input_df = input_df.withColumn("rpt_prd_end_dt", F.to_date("rpt_prd_end_dt", "yyyy-MM-dd"))

# Define the current reporting month (start of the current month)
current_month = datetime.now().replace(day=1)

# Define the start date for the rolling 12-month period
rolling_12_month_start = current_month - timedelta(days=365)

# Step 1: Filter for current and rolling 12-month periods
current_df = input_df.filter(F.col("rpt_prd_end_dt") == F.lit(current_month))
rolling_df = input_df.filter(
    (F.col("rpt_prd_end_dt") >= F.lit(rolling_12_month_start)) & 
    (F.col("rpt_prd_end_dt") < F.lit(current_month))
)

# Step 2: Calculate current balances (deposits and loans)
current_balances = (
    current_df.groupBy("uen")  # Removed "lvl1_prod_dsc" to avoid duplicate columns
    .agg(
        F.sum(F.when(F.col("lvl1_prod_dsc") == "deposits", F.col("balance") * F.col("exg_rate_val"))).alias("curr_dep_bal_usd"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "deposits", F.col("balance"))).alias("curr_dep_bal_cad"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "loans", F.col("balance") * F.col("exg_rate_val"))).alias("curr_crd_bal_usd"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "loans", F.col("balance"))).alias("curr_crd_bal_cad")
    )
)

# Step 3: Calculate rolling 12-month balances (deposits and loans)
rolling_balances = (
    rolling_df.groupBy("uen")  # Removed "lvl1_prod_dsc" to avoid duplicate columns
    .agg(
        F.sum(F.when(F.col("lvl1_prod_dsc") == "deposits", F.col("balance") * F.col("exg_rate_val"))).alias("prev_dep_bal_usd"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "deposits", F.col("balance"))).alias("prev_dep_bal_cad"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "loans", F.col("balance") * F.col("exg_rate_val"))).alias("prev_crd_bal_usd"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "loans", F.col("balance"))).alias("prev_crd_bal_cad")
    )
)

# Step 4: Calculate product lists
product_lists = (
    rolling_df.groupBy("uen")
    .agg(
        F.collect_set(F.when(F.col("rpt_prd_end_dt") == F.lit(current_month), F.col("prod_nm"))).alias("prd_curr"),
        F.collect_set(F.when((F.col("rpt_prd_end_dt") >= F.lit(rolling_12_month_start)) & (F.col("rpt_prd_end_dt") < F.lit(current_month)), F.col("prod_nm"))).alias("prd_prev")
    )
)

# Step 5: Combine current and rolling balances
# FIX: Added distinct column names in `current_balances` and `rolling_balances` to avoid duplicates
final_df = (
    current_balances
    .join(rolling_balances, on="uen", how="left")  # Joins on "uen", ensuring no duplicate columns
    .join(product_lists, on="uen", how="left")
)

# Step 6: Calculate year-over-year changes and product penetration
final_df = final_df.withColumn(
    "dep_bal_yoy",
    F.when(F.col("prev_dep_bal_usd") != 0, (F.col("curr_dep_bal_usd") - F.col("prev_dep_bal_usd")) / F.col("prev_dep_bal_usd")).otherwise(None)
).withColumn(
    "crd_bal_yoy",
    F.when(F.col("prev_crd_bal_usd") != 0, (F.col("curr_crd_bal_usd") - F.col("prev_crd_bal_usd")) / F.col("prev_crd_bal_usd")).otherwise(None)
).withColumn(
    "prd_penetration",
    F.when(F.size(F.col("prd_prev")) != 0, (F.size(F.col("prd_curr")) - F.size(F.col("prd_prev"))) / F.size(F.col("prd_prev"))).otherwise(None)
)

# Step 7: Add reporting period end date
final_df = final_df.withColumn("rpt_prd_end_dt", F.lit(current_month))

# Write the result to the output dataset
dataiku.spark.write_with_schema(final_df, "output_dataset")
