from pyspark.sql import functions as F

# List of join columns
join_columns = ["column1", "column2", "column3", "column4"]  # Replace with actual column names

# Define a function to clean and normalize columns
def normalize_column(col):
    return F.when(F.col(col).isNotNull(), F.trim(F.lower(F.col(col)))) \
            .otherwise("default_value")  # Replace NULLs with a default value

# Process and normalize columns in both DataFrames
for col in join_columns:
    df = df.withColumn(col, normalize_column(col))
    small_df = small_df.withColumn(col, normalize_column(col))

# Generate join key dynamically for both DataFrames
df = df.withColumn("join_key", F.concat_ws("|", *[F.col(col) for col in join_columns]))
small_df = small_df.withColumn("join_key", F.concat_ws("|", *[F.col(col) for col in join_columns]))

# Perform the join
joined_df = df.join(small_df, on="join_key", how="left")

