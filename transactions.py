# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit, expr

# Start a Spark session
spark = SparkSession.builder.appName("Dataiku Recipe").getOrCreate()

# Read the input dataset
input_dataset = dataiku.Dataset("input_dataset")
input_df = input_dataset.get_dataframe(spark)

# CAD to USD conversion rate
CAD_TO_USD = 0.75

# Calculate current month transaction volume (in CAD and USD)
input_df = input_df.withColumn("inc_trx_curr_cad", col("sum_credit_trans1")) \
                   .withColumn("inc_trx_curr_usd", col("sum_credit_trans1") * lit(CAD_TO_USD))

# Calculate the sum of transactions for the last 12 months (in CAD and USD)
last_12_columns = [f"sum_credit_trans{i}" for i in range(1, 13)]
input_df = input_df.withColumn("inc_trx_prev_cad", sum([col(c) for c in last_12_columns])) \
                   .withColumn("inc_trx_prev_usd", col("inc_trx_prev_cad") * lit(CAD_TO_USD))

# Calculate year-over-year change (if previous 12 months columns exist)
previous_12_columns = [f"sum_credit_trans{i}" for i in range(13, 19)]
if set(previous_12_columns).issubset(input_df.columns):
    input_df = input_df.withColumn("prev_12_cad", sum([col(c) for c in previous_12_columns]))
    input_df = input_df.withColumn(
        "inc_trx_yoy",
        expr("(inc_trx_prev_cad - prev_12_cad) / prev_12_cad * 100")
    )
else:
    input_df = input_df.withColumn("inc_trx_yoy", lit(None))  # Handle missing columns gracefully

# Select required columns for the output
output_columns = [
    "uen",
    "rpt_prd_end_dt",
    "inc_trx_curr_usd",
    "inc_trx_curr_cad",
    "inc_trx_prev_usd",
    "inc_trx_prev_cad",
    "inc_trx_yoy"
]
output_df = input_df.select(*output_columns)

# Write the result to the output dataset
output_dataset = dataiku.Dataset("output_dataset")
output_dataset.write_with_schema(output_df)
