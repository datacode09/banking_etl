from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Load the Dataiku dataset
input_df = dataiku.spark.get_dataframe("input_dataset_name")

# Define the current date and calculate the 12-month-ago date
current_date = datetime.now()
twelve_months_ago = current_date - timedelta(days=365)

# Convert the date column to date type if necessary
input_df = input_df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

# Create a window specification for the last 12 months
window_spec = Window.partitionBy("primary_key").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Filter rows where col1 equals the desired string
filtered_df = input_df.filter(
    (F.col("col1") == "VALUE") & 
    (F.col("date") >= F.lit(twelve_months_ago.strftime("%Y-%m-%d")))
)

# Calculate the total sum of numerical_value over the last 12 months
filtered_df = filtered_df.withColumn(
    "total_sum_last_12_months",
    F.sum("numerical_value").over(window_spec)
)

# Join the result back to the main DataFrame to add the new column
result_df = input_df.join(
    filtered_df.select("primary_key", "total_sum_last_12_months").distinct(),
    on="primary_key",
    how="left"
)

# Write the result back to a Dataiku dataset
dataiku.spark.write_with_schema(result_df, "output_dataset_name")
