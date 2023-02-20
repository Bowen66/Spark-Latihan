from pyspark.sql import SparkSession
from pyspark.sql import Row

#Create a spark session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fhv_tripdata_2019-01.csv")
df.printSchema()

df.createOrReplaceTempView("tripData")
sqlDF = spark.sql("SELECT DISTINCT dispatching_base_num, pickup_datatime, dropoff_datatime FROM tripData where PULocationID is not null and DOLocationID ='264' and dispatching_base_num= 'B02182' GROUP BY dispatching_base_num,pickup_datetime, dropoff_datetime ORDER BY pickup_datetime ASC")
sqlDF.show()