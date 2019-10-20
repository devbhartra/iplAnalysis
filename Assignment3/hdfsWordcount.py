# Spark stream to monitor a directory on hdfs and get word count from all csv files in the directory.
# The directory is names "stream_test"

# import files
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Create a spark session. handle name -> spark
spark = SparkSession.builder.appName("Streaming-wordcount").getOrCreate()

# Define schema of csv
userSchema = StructType().add("word","string").add("count","integer")

# Read CSV from HDFS path. Seperator is comma(,). schema is the above defined schema.
dfCSV = spark.readStream.option("sep",",").option("header","false").schema(userSchema).csv("hdfs://localhost:9000/stream_test/")

# Creates temporary view for the given name
dfCSV.createOrReplaceTempView("wordcount")

# SQL query on the data
totalSalary = spark.sql("select word,sum(count) from wordcount group by word")

# Start spark standard streaming query.
query = totalSalary.writeStream.outputMode("complete").format("console").start()

#Loop forever until termination
query.awaitTermination()
