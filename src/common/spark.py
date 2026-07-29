import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    os.environ["PYSPARK_GATEWAY_OPTS"] = "-Divy.log=ERROR"
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "net.snowflake:spark-snowflake_2.12:3.1.9",
                "net.snowflake:snowflake-jdbc:3.24.2"
            ])
        )
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
