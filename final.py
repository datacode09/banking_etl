from pyspark.sql import functions as F
from datetime import datetime, timedelta

# -----------------------------
# Configuration Section
# -----------------------------
CONFIG = {
    "current_month": datetime.now().replace(day=1),
    "rolling_window_months": 12,
    "columns": {
        "rpt_prd_end_dt": "rpt_prd_end_dt",
        "uen": "uen",
        "lvl1_prd_dsc": "lvl1_prd_dsc",
        "balance": "balance",
        "exg_rate_val": "exg_rate_val",
        "prod_nm": "prod_nm",
    },
    "categories": {
        "deposits": "deposits",
        "loans": "loans",
    },
    "output_columns": {
        "dep_bal_curr_usd": "dep_bal_curr_usd",
        "dep_bal_curr_cad": "dep_bal_curr_cad",
        "dep_bal_prev_usd": "dep_bal_prev_usd",
        "dep_bal_prev_cad": "dep_bal_prev_cad",
        "dep_bal_yoy": "dep_bal_yoy",
        "crd_bal_curr_usd": "crd_bal_curr_usd",
        "crd_bal_curr_cad": "crd_bal_curr_cad",
        "crd_bal_prev_usd": "crd_bal_prev_usd",
        "crd_bal_prev_cad": "crd_bal_prev_cad",
        "crd_bal_yoy": "crd_bal_yoy",
        "prd_curr": "prd_curr",
        "prd_prev": "prd_prev",
        "prd_penetration": "prd_penetration",
    },
    "output_dataset": "output_dataset",
    "source_dataset": "source_dataset",
}

# -----------------------------
# Helper Functions
# -----------------------------
def calculate_balances(df, category, time_filter, agg_columns, suffix):
    """
    Calculate balances for the specified category (deposits or loans).
    
    Parameters:
    - df: Source DataFrame
    - category: 'deposits' or 'loans'
    - time_filter: Filter condition for the time period
    - agg_columns: Dictionary of aggregation columns
    - suffix: Suffix to append to the output column names
    
    Returns:
    - Aggregated DataFrame with balances for the specified category
    """
    filtered_df = df.filter((F.col(CONFIG["columns"]["lvl1_prd_dsc"]) == category) & time_filter)
    return (
        filtered_df.groupBy(CONFIG["columns"]["uen"])
        .agg(
            F.sum(F.col(CONFIG["columns"]["balance"]) * F.col(CONFIG["columns"]["exg_rate_val"])).alias(agg_columns[f"{category}_usd"]),
            F.sum(F.col(CONFIG["columns"]["balance"])).alias(agg_columns[f"{category}_cad"]),
        )
        .withColumnRenamed(agg_columns[f"{category}_usd"], f"{category}_bal_{suffix}_usd")
        .withColumnRenamed(agg_columns[f"{category}_cad"], f"{category}_bal_{suffix}_cad")
    )

def calculate_product_lists(df, time_filter, current_month_filter):
    """
    Calculate product lists for the current month and rolling period.

    Parameters:
    - df: Source DataFrame
    - time_filter: Filter condition for the rolling 12-month period
    - current_month_filter: Filter condition for the current month

    Returns:
    - DataFrame with product lists for current and rolling periods
    """
    return (
        df.filter(time_filter | current_month_filter)
        .groupBy(CONFIG["columns"]["uen"])
        .agg(
            F.collect_set(F.when(current_month_filter, F.col(CONFIG["columns"]["prod_nm"]))).alias(CONFIG["output_columns"]["prd_curr"]),
            F.collect_set(F.when(time_filter, F.col(CONFIG["columns"]["prod_nm"]))).alias(CONFIG["output_columns"]["prd_prev"]),
        )
    )

# -----------------------------
# Main Logic
# -----------------------------
def main():
    # Load the source data
    input_df = dataiku.spark.get_dataframe(CONFIG["source_dataset"])

    # Convert `rpt_prd_end_dt` to a proper date format
    input_df = input_df.withColumn(CONFIG["columns"]["rpt_prd_end_dt"], F.to_date(CONFIG["columns"]["rpt_prd_end_dt"], "yyyy-MM-dd"))

    # Define the time filters
    current_month_filter = F.col(CONFIG["columns"]["rpt_prd_end_dt"]) == F.lit(CONFIG["current_month"])
    rolling_window_start = CONFIG["current_month"] - timedelta(days=CONFIG["rolling_window_months"] * 30)
    rolling_window_filter = (F.col(CONFIG["columns"]["rpt_prd_end_dt"]) >= F.lit(rolling_window_start)) & (F.col(CONFIG["columns"]["rpt_prd_end_dt"]) < F.lit(CONFIG["current_month"]))

    # Calculate balances
    deposits_current = calculate_balances(input_df, CONFIG["categories"]["deposits"], current_month_filter, CONFIG["output_columns"], "curr")
    deposits_rolling = calculate_balances(input_df, CONFIG["categories"]["deposits"], rolling_window_filter, CONFIG["output_columns"], "prev")
    loans_current = calculate_balances(input_df, CONFIG["categories"]["loans"], current_month_filter, CONFIG["output_columns"], "curr")
    loans_rolling = calculate_balances(input_df, CONFIG["categories"]["loans"], rolling_window_filter, CONFIG["output_columns"], "prev")

    # Calculate product lists
    product_lists = calculate_product_lists(input_df, rolling_window_filter, current_month_filter)

    # Combine all metrics
    final_df = (
        deposits_current.join(deposits_rolling, on=CONFIG["columns"]["uen"], how="left")
        .join(loans_current, on=CONFIG["columns"]["uen"], how="left")
        .join(loans_rolling, on=CONFIG["columns"]["uen"], how="left")
        .join(product_lists, on=CONFIG["columns"]["uen"], how="left")
    )

    # Calculate YoY and penetration metrics
    final_df = final_df.withColumn(
        CONFIG["output_columns"]["dep_bal_yoy"],
        F.when(
            F.col(CONFIG["output_columns"]["dep_bal_prev_usd"]) != 0,
            (F.col(CONFIG["output_columns"]["dep_bal_curr_usd"]) - F.col(CONFIG["output_columns"]["dep_bal_prev_usd"])) / F.col(CONFIG["output_columns"]["dep_bal_prev_usd"]),
        ).otherwise(None),
    ).withColumn(
        CONFIG["output_columns"]["crd_bal_yoy"],
        F.when(
            F.col(CONFIG["output_columns"]["crd_bal_prev_usd"]) != 0,
            (F.col(CONFIG["output_columns"]["crd_bal_curr_usd"]) - F.col(CONFIG["output_columns"]["crd_bal_prev_usd"])) / F.col(CONFIG["output_columns"]["crd_bal_prev_usd"]),
        ).otherwise(None),
    ).withColumn(
        CONFIG["output_columns"]["prd_penetration"],
        F.when(
            F.size(F.col(CONFIG["output_columns"]["prd_prev"])) != 0,
            (F.size(F.col(CONFIG["output_columns"]["prd_curr"])) - F.size(F.col(CONFIG["output_columns"]["prd_prev"]))) / F.size(F.col(CONFIG["output_columns"]["prd_prev"])),
        ).otherwise(None),
    ).withColumn(CONFIG["columns"]["rpt_prd_end_dt"], F.lit(CONFIG["current_month"]))

    # Write to output dataset
    dataiku.spark.write_with_schema(final_df, CONFIG["output_dataset"])

# Execute main
if __name__ == "__main__":
    main()
