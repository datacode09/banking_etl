df = df.withColumn("join_key", F.sha2(F.concat_ws("|", F.col("column1"), F.col("column2"), F.col("column3"), F.col("column4")), 256))
small_df = small_df.withColumn("join_key", F.sha2(F.concat_ws("|", F.col("column1"), F.col("column2"), F.col("column3"), F.col("column4")), 256))
joined_df = large_df.join(small_df, on="join_key", how="left")
