from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext
import requests

sparkSession = (SparkSession
                .builder
                .appName('spark_with_hive')
                .config("hive.metastore.uris", "thrift://localhost:9083")
		.config("spark.sql.warehouse.dir","/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate()
                )

api_endpoint = "https://api.github.com/repos/apache/spark/commits?since=2022-01-01&until=2022-01-23"
# api_params = {"start_date": "2022-01-01", "end_date": "2022-01-31"}
# make a request to the API
response = requests.get(api_endpoint)
# create a PySpark DataFrame from the API response
data = response.json()
df = sparkSession.createDataFrame(data)

# transform the DataFrame as needed
df.write.mode('overwrite').saveAsTable('test_db.github_data')

df2=sparkSession.sql('select * from test_db.github_data')

df2.show()
