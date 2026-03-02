from pyspark.sql import SparkSession

def get_spark_session():
    return SparkSession.builder \
        .appName("SparkETLPipeline") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def extract_data(input_path):
    spark = get_spark_session()

    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    print("Data extracted successfully")
    df.printSchema()

    return df
