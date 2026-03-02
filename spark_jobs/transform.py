from pyspark.sql.functions import col, sum

def transform_data(df):
    # Drop null values
    df = df.dropna()

    # Example transformation: calculate total sales
    df = df.withColumn(
        "sales_amount",
        col("quantity") * col("price")
    )

    # Aggregate sales by region
    aggregated_df = df.groupBy("region") \
        .agg(sum("sales_amount").alias("total_sales"))

    print("Data transformed successfully")

    return aggregated_df
