from pyspark.sql import functions as F
from datetime import datetime

# Load the source data
input_df = dataiku.spark.get_dataframe("source_dataset")

# Convert `rpt_prd_end_dt` to a proper date format
input_df = input_df.withColumn("rpt_prd_end_dt", F.to_date("rpt_prd_end_dt", "yyyy-MM-dd"))

# Step 1: Identify the last reporting date for each UEN
last_reporting_date = (
    input_df.groupBy("uen")
    .agg(F.max("rpt_prd_end_dt").alias("last_rpt_prd_end_dt"))
)

# Step 2: Join the last reporting date to the source data
input_with_last_date = input_df.join(last_reporting_date, on="uen", how="left")

# Step 3: Calculate the rolling lookback window for each UEN
# The rolling lookback window is defined as [last_rpt_prd_end_dt - 12 months, last_rpt_prd_end_dt]
rolling_df = input_with_last_date.filter(
    (F.col("rpt_prd_end_dt") <= F.col("last_rpt_prd_end_dt")) &
    (F.col("rpt_prd_end_dt") > F.date_sub(F.col("last_rpt_prd_end_dt"), 365))
)

# Step 4: Calculate metrics for the last reporting date
# Current balances (for the most recent `rpt_prd_end_dt`)
current_balances = (
    input_with_last_date.filter(F.col("rpt_prd_end_dt") == F.col("last_rpt_prd_end_dt"))
    .groupBy("uen")
    .agg(
        F.sum(F.when(F.col("lvl1_prod_dsc") == "deposits", F.col("balance") * F.col("exg_rate_val"))).alias("dep_bal_usd"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "deposits", F.col("balance"))).alias("dep_bal_cad"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "loans", F.col("balance") * F.col("exg_rate_val"))).alias("crd_bal_usd"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "loans", F.col("balance"))).alias("crd_bal_cad")
    )
)

# Rolling metrics (aggregated over the rolling lookback window)
rolling_balances = (
    rolling_df.groupBy("uen")
    .agg(
        F.sum(F.when(F.col("lvl1_prod_dsc") == "deposits", F.col("balance") * F.col("exg_rate_val"))).alias("rolling_dep_bal_usd"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "deposits", F.col("balance"))).alias("rolling_dep_bal_cad"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "loans", F.col("balance") * F.col("exg_rate_val"))).alias("rolling_crd_bal_usd"),
        F.sum(F.when(F.col("lvl1_prod_dsc") == "loans", F.col("balance"))).alias("rolling_crd_bal_cad")
    )
)

# Product lists (current and rolling)
product_lists = (
    rolling_df.groupBy("uen")
    .agg(
        F.collect_set("prod_nm").alias("rolling_prd_list")
    )
)

# Combine current and rolling metrics into a single DataFrame
final_df = (
    current_balances
    .join(rolling_balances, on="uen", how="left")
    .join(product_lists, on="uen", how="left")
    .join(last_reporting_date, on="uen", how="left")
)

# Add data availability indicator
final_df = final_df.withColumn(
    "data_availability_indicator",
    F.when((F.col("dep_bal_usd") > 0) | (F.col("crd_bal_usd") > 0), 1).otherwise(0)
)

# Write the result to the output dataset
dataiku.spark.write_with_schema(final_df, "output_dataset")
