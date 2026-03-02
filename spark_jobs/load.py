def load_data(df, output_path):
    df.write \
        .mode("overwrite") \
        .partitionBy("region") \
        .parquet(output_path)

    print("Data loaded successfully in Parquet format")
