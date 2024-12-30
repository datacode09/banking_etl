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

# Step 2: Filter the data for the last reporting date per UEN
last_report_df = input_df.join(last_reporting_date, 
                               (input_df["uen"] == last_reporting_date["uen"]) &
                               (input_df["rpt_prd_end_dt"] == last_reporting_date["last_rpt_prd_end_dt"]),
                               how="inner")

# Step 3: Calculate metrics for the last reporting date
# Current balances (deposits and loans) with missing value checks
current_balances = (
    last_report_df.groupBy("uen")
    .agg(
        F.sum(F.when((F.col("lvl1_prod_dsc") == "deposits") & F.col("balance").isNotNull() & F.col("exg_rate_val").isNotNull(), 
                      F.col("balance") * F.col("exg_rate_val"))).alias("dep_bal_usd"),
        F.sum(F.when((F.col("lvl1_prod_dsc") == "deposits") & F.col("balance").isNotNull(), 
                      F.col("balance"))).alias("dep_bal_cad"),
        F.sum(F.when((F.col("lvl1_prod_dsc") == "loans") & F.col("balance").isNotNull() & F.col("exg_rate_val").isNotNull(), 
                      F.col("balance") * F.col("exg_rate_val"))).alias("crd_bal_usd"),
        F.sum(F.when((F.col("lvl1_prod_dsc") == "loans") & F.col("balance").isNotNull(), 
                      F.col("balance"))).alias("crd_bal_cad")
    )
)

# Product lists with missing value checks
product_lists = (
    last_report_df.groupBy("uen")
    .agg(
        F.collect_set(F.when(F.col("prod_nm").isNotNull(), F.col("prod_nm"))).alias("prd_list")
    )
)

# Step 4: Combine metrics into a single DataFrame
final_df = (
    current_balances
    .join(product_lists, on="uen", how="left")
    .join(last_reporting_date, on="uen", how="left")
)

# Step 5: Add data availability indicator
final_df = final_df.withColumn(
    "data_availability_indicator",
    F.when((F.col("dep_bal_usd").isNotNull() & (F.col("dep_bal_usd") > 0)) | 
           (F.col("crd_bal_usd").isNotNull() & (F.col("crd_bal_usd") > 0)), 1).otherwise(0)
)

# Write the result to the output dataset
dataiku.spark.write_with_schema(final_df, "output_dataset")
