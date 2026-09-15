from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date


def main():
    spark = (
        SparkSession.builder
        .appName("A10_Clickstream_Transform")
        .getOrCreate()
    )

    input_path = "/opt/airflow/data/input"
    output_path = "/opt/airflow/data/output"

    print(f"Reading input data from: {input_path}")

    events_df = spark.read.json(input_path)

    events_clean_df = (
        events_df
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp"))
        )
        .withColumn(
            "event_date",
            to_date(col("event_timestamp"))
        )
        .dropna(subset=["event_id", "user_id", "event_timestamp"])
    )

    print("Cleaned event count:")
    print(events_clean_df.count())

    print("Writing transformed data...")

    (
        events_clean_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(output_path)
    )

    print(f"Transformation completed. Output: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()