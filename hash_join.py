from pyspark.sql import functions as F

# List of join columns
join_columns = ["column1", "column2", "column3", "column4"]  # Replace with your actual columns

# Generate join key dynamically for the large DataFrame
df = df.withColumn("join_key", F.sha2(F.concat_ws("|", *[F.col(col) for col in join_columns]), 256))

# Generate join key dynamically for the small DataFrame
small_df = small_df.withColumn("join_key", F.sha2(F.concat_ws("|", *[F.col(col) for col in join_columns]), 256))

joined_df = large_df.join(small_df, on="join_key", how="left")
