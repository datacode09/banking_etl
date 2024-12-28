from pyspark.sql import functions as F

# Load the dataset
input_df = dataiku.spark.get_dataframe("input_dataset_name")

# Convert rpt_prd_end_date to date type if necessary
input_df = input_df.withColumn("rpt_prd_end_date", F.to_date("rpt_prd_end_date", "yyyy-MM-dd"))

# Group products into a list for each uen and rpt_prd_end_date
result_df = (
    input_df
    .groupBy("uen", "rpt_prd_end_date")
    .agg(F.collect_list("product_name").alias("product_list"))
)

# Optionally, convert the product list to a comma-separated string
result_df = result_df.withColumn("product_list", F.concat_ws(", ", F.col("product_list")))

# Write the result back to a Dataiku dataset
dataiku.spark.write_with_schema(result_df, "output_dataset_name")
