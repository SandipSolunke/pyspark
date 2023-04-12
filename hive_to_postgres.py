from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import col
import requests

sparkSession = (SparkSession
                .builder
                .appName('spark_with_hive')
                .config("hive.metastore.uris", "thrift://localhost:9083")
		.config("spark.sql.warehouse.dir","/user/hive/warehouse")
		.config("spark.jars", "postgresql-42.6.0.jar")
                .enableHiveSupport()
                .getOrCreate()
                )


df=sparkSession.sql('select * from test_db.github_data');

df.show()
df = df.select([col(c).cast("string") for c in df.columns])

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/hive_data") \
    .option("dbtable", "github_data") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

# stop the SparkSession
sparkSession.stop()
