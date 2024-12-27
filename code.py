from pyspark.sql import functions as F

# Load input dataset
input_df = spark.sql("SELECT * FROM account_universe")

# Step 1: Calculate Deposit and Credit Balances
output_df = (
    input_df
    .withColumn("dep_bal_curr_usd", 
                F.when(F.col("ccy") == "USD", F.col("balance"))
                 .otherwise(F.col("balance") * F.lit(1.25)))  # Assume 1.25 as conversion rate for CAD -> USD
    .withColumn("dep_bal_curr_cad", 
                F.when(F.col("ccy") == "CAD", F.col("balance"))
                 .otherwise(F.col("balance") / F.lit(1.25)))  # Assume 1.25 as conversion rate for USD -> CAD
    .withColumn("dep_bal_prev_usd", 
                F.col("balance") * F.lit(0.9))  # Assume 90% of current balance as a placeholder for previous data
    .withColumn("dep_bal_prev_cad", 
                F.col("balance") * F.lit(0.9))  # Same assumption for CAD
    .withColumn("dep_bal_yoy_change_usd", 
                ((F.col("dep_bal_curr_usd") - F.col("dep_bal_prev_usd")) / F.col("dep_bal_prev_usd")) * 100)
    .withColumn("dep_bal_yoy_change_cad", 
                ((F.col("dep_bal_curr_cad") - F.col("dep_bal_prev_cad")) / F.col("dep_bal_prev_cad")) * 100)
    .withColumn("crd_bal_curr_usd", 
                F.when(F.col("ccy") == "USD", F.col("balance"))
                 .otherwise(F.col("balance") * F.lit(1.25)))  # Same conversion for credit balances
    .withColumn("crd_bal_curr_cad", 
                F.when(F.col("ccy") == "CAD", F.col("balance"))
                 .otherwise(F.col("balance") / F.lit(1.25)))
    .withColumn("crd_bal_prev_usd", 
                F.col("balance") * F.lit(0.8))  # Assume 80% of current balance as previous data for credit
    .withColumn("crd_bal_prev_cad", 
                F.col("balance") * F.lit(0.8))
    .withColumn("crd_bal_yoy_change_usd", 
                ((F.col("crd_bal_curr_usd") - F.col("crd_bal_prev_usd")) / F.col("crd_bal_prev_usd")) * 100)
    .withColumn("crd_bal_yoy_change_cad", 
                ((F.col("crd_bal_curr_cad") - F.col("crd_bal_prev_cad")) / F.col("crd_bal_prev_cad")) * 100)
)

# Step 2: Calculate Incoming Transaction Volume
output_df = output_df.withColumn(
    "inc_txn_vol_curr_usd",
    F.sum(F.when(F.col("ccy") == "USD", F.col("txn_vol"))).over(Window.partitionBy("uen"))
).withColumn(
    "inc_txn_vol_curr_cad",
    F.sum(F.when(F.col("ccy") == "CAD", F.col("txn_vol"))).over(Window.partitionBy("uen"))
).withColumn(
    "inc_txn_vol_prev_usd",
    F.col("txn_vol") * F.lit(0.85)  # Assume 85% of transaction volume as previous period
).withColumn(
    "inc_txn_vol_prev_cad",
    F.col("txn_vol") * F.lit(0.85)
).withColumn(
    "inc_txn_vol_yoy_change_usd",
    ((F.col("inc_txn_vol_curr_usd") - F.col("inc_txn_vol_prev_usd")) / F.col("inc_txn_vol_prev_usd")) * 100
).withColumn(
    "inc_txn_vol_yoy_change_cad",
    ((F.col("inc_txn_vol_curr_cad") - F.col("inc_txn_vol_prev_cad")) / F.col("inc_txn_vol_prev_cad")) * 100
)

# Step 3: Calculate Product Metrics
output_df = output_df.withColumn(
    "prod_cnt", 
    F.size(F.array(F.col("lvl1_prod_dsc"), F.col("lvl2_prod_dsc"), F.col("lvl3_prod_dsc"), F.col("lvl4_prod_dsc")))
).withColumn(
    "prod_pen",
    F.col("prod_cnt") / F.lit(10)  # Assuming total_possible_products is 10 (replace with actual value if available)
)

# Step 4: Select and Rename Columns to Match Output Schema
output_df = output_df.select(
    F.col("uen").alias("Unique Identifier"),
    F.col("rpt_prd_end_dt").alias("Report Period End Date"),
    F.col("dep_bal_curr_usd").alias("Deposit Balance in USD"),
    F.col("dep_bal_curr_cad").alias("Deposit Balance in CAD"),
    F.col("dep_bal_yoy_change_usd").alias("Year-over-Year Deposit Change in USD"),
    F.col("dep_bal_yoy_change_cad").alias("Year-over-Year Deposit Change in CAD"),
    F.col("crd_bal_curr_usd").alias("Credit Balance in USD"),
    F.col("crd_bal_curr_cad").alias("Credit Balance in CAD"),
    F.col("crd_bal_yoy_change_usd").alias("Year-over-Year Credit Change in USD"),
    F.col("crd_bal_yoy_change_cad").alias("Year-over-Year Credit Change in CAD"),
    F.col("inc_txn_vol_curr_usd").alias("Incoming Transaction Volume in USD"),
    F.col("inc_txn_vol_curr_cad").alias("Incoming Transaction Volume in CAD"),
    F.col("inc_txn_vol_yoy_change_usd").alias("Year-over-Year Transaction Volume Change in USD"),
    F.col("inc_txn_vol_yoy_change_cad").alias("Year-over-Year Transaction Volume Change in CAD"),
    F.col("prod_cnt").alias("Current Product Count"),
    F.col("prod_pen").alias("Product Penetration")
)

# Save the transformed dataset
output_df.write.format("parquet").mode("overwrite").save("/path/to/output/dataset")
