'''
Spark streaming using pyspark to find out the different types of clients using twitter and their count from the given dataset.
The dataset (fifa_modded_small_1.csv) comprises of collection of tweets during 2018 FIFA world cup.
The Columns of the dataset are:
ID,Lang,Date,Source,len,Likes,RT's,Hashtags,UserMentionNames,UserMentionID,Name,Place,Followers,Friends
The file is being read from /stream directory on HDFS.
'''
# import files
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Create a spark session. handle name -> spark
spark = SparkSession.builder.appName("Streaming-clientcount").getOrCreate()

# Define schema of csv
userSchema = StructType()\
	.add("ID","string")\
	.add("Lang","string")\
	.add("Date","string")\
	.add("Source","string")\
	.add("Len","integer")\
	.add("Likes","integer")\
	.add("RT","integer")\
	.add("Hashtags","string")\
	.add("MentionNames","string")\
	.add("MentionID","string")\
	.add("Name","string")\
	.add("Place","string")\
	.add("Followers","integer")\
	.add("Friends","integer")	

# Read CSV from HDFS path. Seperator is comma(,). schema is the above defined schema.
dfCSV = spark.readStream.option("sep",";").option("header","false").schema(userSchema).csv("hdfs://localhost:9000/stream/")

# Creates temporary view for the given name
dfCSV.createOrReplaceTempView("clientCount")

print("\n\ndfCSV contains: ",dfCSV)

# SQL query on the data
allSources = spark.sql("select Source,count(Source) as client_count from commonHashtag group by Source")

# Start spark standard streaming query.
query = allSources.writeStream.outputMode("complete").format("console").start()

#Terminate after 40 seconds
query.awaitTermination(40)
