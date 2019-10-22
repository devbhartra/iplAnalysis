# Assignment 3 task-1, Big data [UE17CS313], PES University

'''
Spark streaming using pyspark to find out the most common hastag from the given dataset.
The dataset (fifa_modded_small_1.csv) comprises of collection of tweets during 2018 FIFA world cup.
The Columns of the dataset are:
ID,Lang,Date,Source,len,Likes,RT's,Hashtags,UserMentionNames,UserMentionID,Name,Place,Followers,Friends
The file is being read from /stream directory on HDFS.
'''
# import files
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import split, explode, col

# Create a spark session. handle name -> spark
spark = SparkSession.builder.appName("Streaming-commonHashtag").getOrCreate()

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
dfCSV.createOrReplaceTempView("commonHashtag")

print("\ndfCSV contains: ",dfCSV)
print("\ntype of dfCSV: ",type(dfCSV))

# SQL query on the data

allTags = spark.sql("select Hashtags from commonHashtag")

hashtags = allTags.select(explode(split(allTags['Hashtags'],",")).alias("tag"))	

hashTagCount = hashtags.groupBy("tag").count().orderBy(col("count").desc())

# Start spark standard streaming query.
query = hashTagCount.writeStream.outputMode("complete").format("console").start()

print("\nallTags type: ",type(allTags))
print("\nhashtag type: ",type(hashtags))
print("\nhashTagCount list type: ",type(hashTagCount))

#Terminate after 100 seconds
query.awaitTermination(100)
