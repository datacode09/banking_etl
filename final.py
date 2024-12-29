from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load the source data
input_df = dataiku.spark.get_dataframe("input_dataset_name")

# Ensure the `rpt_prd_end_date` is in the correct format
input_df = input_df.withColumn("rpt_prd_end_date", F.to_date("rpt_prd_end_date", "yyyy-MM-dd"))

# Step 1: Filter data for deposits and loans
deposit_df = input_df.filter(F.col("level_1_product_description") == "deposit")
loan_df = input_df.filter(F.col("level_1_product_description") == "loan")

# Step 2: Convert balances to USD and CAD
deposit_df = deposit_df.withColumn("balance_usd", F.col("balance") * F.col("cad_to_us_exchange_rate"))
loan_df = loan_df.withColumn("balance_usd", F.col("balance") * F.col("cad_to_us_exchange_rate"))

# Step 3: Aggregate balances for the current and previous 12 months
current_year_window = Window.partitionBy("uen").orderBy(F.col("rpt_prd_end_date").desc())
previous_year_window = Window.partitionBy("uen").orderBy(F.col("rpt_prd_end_date").asc())

# Aggregate deposits for the current period
deposit_current = (
    deposit_df.filter(F.year(F.col("rpt_prd_end_date")) == F.year(F.current_date()))
    .groupBy("uen", "rpt_prd_end_date")
    .agg(
        F.sum("balance_usd").alias("dep_bal_curr_usd"),
        F.sum("balance").alias("dep_bal_curr_cad"),
    )
)

# Aggregate deposits for the previous year
deposit_prev = (
    deposit_df.filter(F.year(F.col("rpt_prd_end_date")) == F.year(F.current_date()) - 1)
    .groupBy("uen")
    .agg(
        F.sum("balance_usd").alias("dep_bal_prev_usd"),
        F.sum("balance").alias("dep_bal_prev_cad"),
    )
)

# Aggregate loans for the current period
loan_current = (
    loan_df.filter(F.year(F.col("rpt_prd_end_date")) == F.year(F.current_date()))
    .groupBy("uen", "rpt_prd_end_date")
    .agg(
        F.sum("balance_usd").alias("crd_bal_curr_usd"),
        F.sum("balance").alias("crd_bal_curr_cad"),
    )
)

# Aggregate loans for the previous year
loan_prev = (
    loan_df.filter(F.year(F.col("rpt_prd_end_date")) == F.year(F.current_date()) - 1)
    .groupBy("uen")
    .agg(
        F.sum("balance_usd").alias("crd_bal_prev_usd"),
        F.sum("balance").alias("crd_bal_prev_cad"),
    )
)

# Step 4: Calculate year-over-year changes for deposits and loans
yoy_change = deposit_current.join(
    deposit_prev, on="uen", how="left"
).join(
    loan_current, on=["uen", "rpt_prd_end_date"], how="left"
).join(
    loan_prev, on="uen", how="left"
)

yoy_change = yoy_change.withColumn(
    "dep_bal_yoy",
    F.when(F.col("dep_bal_prev_usd").isNotNull(), (F.col("dep_bal_curr_usd") - F.col("dep_bal_prev_usd")) / F.col("dep_bal_prev_usd"))
    .otherwise(None)
)

yoy_change = yoy_change.withColumn(
    "crd_bal_yoy",
    F.when(F.col("crd_bal_prev_usd").isNotNull(), (F.col("crd_bal_curr_usd") - F.col("crd_bal_prev_usd")) / F.col("crd_bal_prev_usd"))
    .otherwise(None)
)

# Step 5: Aggregate product lists
product_agg = input_df.groupBy("uen").agg(
    F.collect_set(F.when(F.year(F.col("rpt_prd_end_date")) == F.year(F.current_date()), F.col("prod_nm"))).alias("prd_curr"),
    F.collect_set(F.when(F.year(F.col("rpt_prd_end_date")) == F.year(F.current_date()) - 1, F.col("prod_nm"))).alias("prd_prev"),
)

# Step 6: Calculate product penetration
result_df = yoy_change.join(product_agg, on="uen", how="left")
result_df = result_df.withColumn(
    "prd_penetration",
    F.when(F.size(F.col("prd_prev")) > 0, (F.size(F.col("prd_curr")) - F.size(F.col("prd_prev"))) / F.size(F.col("prd_prev"))).otherwise(None)
)

# Step 7: Write to Dataiku dataset
dataiku.spark.write_with_schema(result_df, "output_dataset_name")
