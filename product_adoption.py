from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Load the dataset
input_df = dataiku.spark.get_dataframe("input_dataset_name")

# Get the current year
current_year = datetime.now().year
previous_year = current_year - 1

# Convert rpt_prd_end_date to date type if necessary
input_df = input_df.withColumn("rpt_prd_end_date", F.to_date("rpt_prd_end_date", "yyyy-MM-dd"))

# Add year and a ranking column
window_spec = Window.partitionBy("uen")

processed_df = (
    input_df
    # Add a year column to filter for specific years
    .withColumn("year", F.year("rpt_prd_end_date"))
    # Filter only rows from the previous year or current year
    .filter((F.col("year") == previous_year) | (F.col("year") == current_year))
    # Add ranking based on the conditions
    .withColumn(
        "row_rank",
        F.row_number().over(
            window_spec.orderBy(
                F.when(F.col("year") == previous_year, F.lit(1)).otherwise(F.lit(2)),  # Prioritize previous year
                F.col("rpt_prd_end_date").desc()  # Sort by date descending
            )
        )
    )
    # Keep only the top 2 rows per uen
    .filter(F.col("row_rank") <= 2)
)

# Drop unnecessary columns (optional)
final_df = processed_df.drop("year", "row_rank")

# Write the result back to a Dataiku dataset
dataiku.spark.write_with_schema(final_df, "output_dataset_name")
