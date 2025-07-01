from pyspark.context import SparkContext
from pyspark import SparkFiles
from pyspark.sql.session import SparkSession
spark = (SparkSession.builder.master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.driver.memory", "4g")  # Increase to 4GB
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.sql.shuffle.partitions", "200")  # Reduce shuffle partitions
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
    .config("spark.executor.memory", "4g")  # If using executors
    .getOrCreate())

data = spark.range(0, 5)
data.write.format("delta").save("/delta_lake/test")
df = spark.read.format("delta").load("/delta_lake/test")
df.show()
spark.stop()