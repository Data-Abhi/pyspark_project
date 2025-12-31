from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("LocalPySparkProject")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
