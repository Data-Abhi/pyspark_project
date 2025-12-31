from src.main.utility.snowflake_session import get_snowflake_spark_session
from src.main.utility.logging_config import logger

def write_df_to_snowflake(df, table_name, mode="overwrite"):
    spark, sf_options = get_snowflake_spark_session()

    (
        df.write
        .format("snowflake")
        .options(**sf_options)
        .option("dbtable", table_name)
        .mode(mode)
        .save()
    )

    logger.info("Data written to Snowflake table: %s", table_name)
