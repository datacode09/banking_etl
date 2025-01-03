from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Load the source data
input_df = dataiku.spark.get_dataframe("source_dataset")

# Convert `rpt_prd_end_dt` to a proper date format
input_df = input_df.withColumn("rpt_prd_end_dt", F.to_date("rpt_prd_end_dt", "yyyy-MM-dd"))

# User-defined reporting date (for testing flexibility)
user_supplied_date = "2024-12-31"
current_month = datetime.strptime(user_supplied_date, "%Y-%m-%d").replace(day=1)

# Define the rolling 12-month period excluding the current month
rolling_12_month_start = current_month - timedelta(days=365)

# Step 1: Calculate current month's metrics
current_df = input_df.filter(F.col("rpt_prd_end_dt") == F.lit(current_month))

current_metrics = (
    current_df.groupBy("uen")
    .agg(
        F.sum(F.col("sum_credit_trans1") * F.col("exg_rate_val")).alias("inc_trx_curr_usd"),
        F.sum(F.col("sum_credit_trans1")).alias("inc_trx_curr_cad")
    )
)

# Step 2: Calculate rolling 12-month metrics excluding current month
rolling_df = input_df.filter(
    (F.col("rpt_prd_end_dt") >= F.lit(rolling_12_month_start)) &
    (F.col("rpt_prd_end_dt") < F.lit(current_month))
)

rolling_metrics = (
    rolling_df.groupBy("uen")
    .agg(
        F.sum(F.col("sum_credit_trans1") * F.col("exg_rate_val")).alias("inc_trx_prev_usd"),
        F.sum(F.col("sum_credit_trans1")).alias("inc_trx_prev_cad")
    )
)

# Step 3: Combine both metrics into the final dataframe
final_df = (
    current_metrics
    .join(rolling_metrics, on="uen", how="left")
    .fillna({
        "inc_trx_curr_usd": 0,
        "inc_trx_curr_cad": 0,
        "inc_trx_prev_usd": 0,
        "inc_trx_prev_cad": 0
    })
)

# Display the result
final_df.show()

# Write the result to the output dataset
dataiku.spark.write_with_schema(final_df, "output_dataset")
