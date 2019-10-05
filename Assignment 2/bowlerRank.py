# Big data assignment-2 [UE17CS313] Sem-5, PES University, 2019.
'''
Ranking bowlers from the IPL using the PageRank algorithm.
Each player is equivalent to a page (as per the PageRank algorithm)
Initial rank of each player is max(sum(bowling_average_against_all_players), 1)
Links are between every batsman-bowler pair as given in the dataset
'''

from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
import sys
import findspark

findspark.init()

def sort_output(current):
        current.sort(key = lambda x: x[1], reverse = True)
        return current


def computeContribs(merged_tuple):
	batsman_list = merged_tuple[1][0]	#list of batsman the bowler has bowled to
	current_rank = merged_tuple[1][1]	#the bowlers current rank
	batsman_num = len(batsman_list)

	for n in batsman_list:
		yield(n, current_rank/batsman_num)

if __name__ == "__main__":
	spark = SparkSession.builder.appName("bowlerRank").getOrCreate()
	sc = spark.sparkContext

	lines = spark.read.text("hdfs://localhost:9000/input/BowlerRankTestData.txt").rdd.map(lambda r: r[0])

	# every bowler with the set of players who faced him ***links go from batsman to bowler***
	links = lines.map(lambda x: x.split(',')).map(lambda x: (x[1],x[0])).groupByKey().cache()

	# using max(sum(averages),1) for each bowler's inital rank
	#initial_ranks = lines.map(lambda x: x.split(',')).map(lambda x: (x[1],1) if ((int(x[2])/int(x[3])) < 1) else (x[1],int(x[2])/int(x[3])) ).reduceByKey(add)
	initial_ranks = lines.map(lambda x: x.split(',')).map(lambda x: (x[1],int(x[2])/int(x[3]))).reduceByKey(add)

	ranks = initial_ranks.map(lambda x: (x[0],1) if (x[1]<1) else (x[0],x[1]))

	initial_ranks_output = ranks.collect()		#collect() action causes all transformations in the above line

	# Print INITIAL RANK LIST and length
	print("\n")
	for (bowler,avg) in initial_ranks_output:
		print(bowler,avg)
	print("\n")

	print("\n Ranks Count:",initial_ranks.count(),"\n\n")

	links_output = links.collect()

	# Print INITIAL LINK LIST and length
	print("\n")
	for (bowler,l) in links_output:
		print(bowler,list(l))
	print("\n")

	print("\n Link Count:",links.count(),"\n\n")

	# Compute new ranks

	for i in range(int(sys.argv[1])):

		contribs = links.join(ranks).flatMap(lambda x: computeContribs(x))

		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank*0.80 + 0.20)

		new_ranks_output = ranks.collect()


	sorted_output = sort_output(new_ranks_output)

	# Print NEW RANKS
	print("\n")
	for (bowler,rank) in sorted_output:
		print(bowler,rank)
	print("\n")

	print("\n Final count: ",ranks.count(),"\n\n")

	spark.stop()

	'''
	counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
	output = pair_list.collect()
	sorted_output = sort_output(output)



	df = spark.createDataFrame(sorted_output,["Word","count"])
	df.show()
	df.printSchema()
	#df.coalesce(1).write.format("csv").save('hdfs://localhost:9000/output/wordcountresult')
	spark.stop()
	'''
