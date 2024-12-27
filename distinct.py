from pyspark.sql import functions as F

# Load input dataset
input_df = dataiku.spark.get_dataframe("input_dataset_name")

# Step 1: Select specific columns
selected_columns = ["column1", "column2", "column3", "column4"]  # Replace with actual column names
selected_df = input_df.select(selected_columns)

# Step 2: Extract distinct values
distinct_df = selected_df.distinct()

# Write output dataset back to Dataiku
dataiku.spark.write_with_schema(distinct_df, "output_dataset_name")
