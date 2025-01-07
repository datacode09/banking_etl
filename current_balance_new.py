from pyspark.sql import functions as F

# Filtering the current month's data
current_df = input_df.filter(F.col("rpt_prd_end_dt") == F.lit(current_month))

# Calculating current balances with complex conditions for deposits and loans using case-insensitive checks
current_balances = (
    current_df.groupBy("uen")
    .agg(
        # Current Deposit Balances in USD
        F.sum(F.when(
            (F.col("lvl1_prod_dsc").contains("deposits")) & 
            (~F.lower(F.col("lvl2_prod_dsc")).contains("revolving")) &
            (F.col("ccy") == "CAD"),
            F.col("balance") * F.col("exg_rate_val")
        ).when(
            (F.col("lvl1_prod_dsc").contains("deposits")) & 
            (~F.lower(F.col("lvl2_prod_dsc")).contains("revolving")) & 
            (F.col("ccy") == "USD"),
            F.col("balance")
        )).alias("curr_dep_bal_usd"),

        # Current Deposit Balances in CAD
        F.sum(F.when(
            (F.col("lvl1_prod_dsc").contains("deposits")) & 
            (~F.lower(F.col("lvl2_prod_dsc")).contains("revolving")) &
            (F.col("ccy") == "CAD"),
            F.col("balance")
        ).when(
            (F.col("lvl1_prod_dsc").contains("deposits")) & 
            (~F.lower(F.col("lvl2_prod_dsc")).contains("revolving")) & 
            (F.col("ccy") == "USD"),
            F.col("balance") / F.col("exg_rate_val")
        )).alias("curr_dep_bal_cad"),

        # Current Credit Balances in USD
        F.sum(F.when(
            (F.col("lvl1_prod_dsc").contains("loans")) & 
            (~F.lower(F.col("lvl2_prod_dsc")).contains("revolving")) &
            (F.col("ccy") == "CAD"),
            F.col("balance") * F.col("exg_rate_val")
        ).when(
            (F.col("lvl1_prod_dsc").contains("loans")) & 
            (~F.lower(F.col("lvl2_prod_dsc")).contains("revolving")) & 
            (F.col("ccy") == "USD"),
            F.col("balance")
        )).alias("curr_crd_bal_usd"),

        # Current Credit Balances in CAD
        F.sum(F.when(
            (F.col("lvl1_prod_dsc").contains("loans")) & 
            (~F.lower(F.col("lvl2_prod_dsc")).contains("revolving")) &
            (F.col("ccy") == "CAD"),
            F.col("balance")
        ).when(
            (F.col("lvl1_prod_dsc").contains("loans")) & 
            (~F.lower(F.col("lvl2_prod_dsc")).contains("revolving")) & 
            (F.col("ccy") == "USD"),
            F.col("balance") / F.col("exg_rate_val")
        )).alias("curr_crd_bal_cad")
    )
)

# Show the result for validation
current_balances.show()
