from pyspark.sql import SparkSession
from src.main.utility.logging_config import *
from resources.dev import config


def get_snowflake_spark_session():

    jars = ",".join([
        "C:/Users/abhis/OneDrive/Desktop/Pyspark Project/jars/snowflake-jdbc-3.15.1.jar",
        "C:/Users/abhis/OneDrive/Desktop/Pyspark Project/jars/spark-snowflake_2.12-2.14.0-spark_3.4.jar"
    ])

    spark = (
        SparkSession.builder
        .appName("Snowflake-Test")
        .master("local[*]")
        .config("spark.jars", jars)
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )

    sf_options = {
        "sfURL": config.sf_url,
        "sfAccount": config.sf_account,
        "sfUser": config.sf_user,
        "sfPassword": config.sf_password,
        "sfDatabase": config.sf_database,
        "sfSchema": config.sf_schema,
        "sfWarehouse": config.sf_warehouse,
        "sfRole": config.sf_role
    }

    return spark, sf_options
