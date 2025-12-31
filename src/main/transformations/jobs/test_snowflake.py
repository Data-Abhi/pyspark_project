from src.main.write.snowflake_write import write_df_to_snowflake
from src.main.utility.snowflake_session import get_snowflake_spark_session

spark, _ = get_snowflake_spark_session()

df = spark.range(10)

write_df_to_snowflake(
    df=df,
    table_name="SPARK_TEST_TABLE",
    mode="overwrite"
)

print("✅ Data written to Snowflake successfully")
