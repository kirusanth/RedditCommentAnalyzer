import nltk
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
import sys
import requests
import re
import os
from nltk import ngrams as nltk_ngrams


# create spark configuration
conf = SparkConf()
conf.setAppName("SentimentCharacters")
# create spark context with the above configuration
sc = SparkContext(conf=conf)

# Create spark session 
spark =  SparkSession.builder \
	.config(conf=sc.getConf()) \
	.getOrCreate()

# input files
episode1 = "../GameofThrones/episode1.csv" 
episode2 = "../GameofThrones/episode2.csv" 
episode3 = "../GameofThrones/episode3full.csv" 
episode4 = "../GameofThrones/episode4.csv" 
episode5 = "../GameofThrones/episode5.csv" 
episode6 = "../GameofThrones/episode6.csv" 
episode7 = "../GameofThrones/episode7full.csv"


# output path
savepath ="../output/Text/Ngrams/"

# list of input files
list_of_inputs = [episode1, episode2, episode3, episode4, episode5, episode6, episode7]

# list of outputfiles
list_of_outputs = ["ngrams_E1", "ngrams_E2", "ngrams_E3", "ngrams_E4", "ngrams_E5", "ngrams_E6", "ngrams_E7"]
# output file format
outputformat = ".txt" #textfile

#size of N gram
N = 5 
season_totals = {}
### For each episode file
for i in range(len(list_of_inputs)):
	# read the files in csv format
	df = spark.read.format("csv") \
		.option("header", "true") \
		.option("inferSchema","true") \
		.load(list_of_inputs[i])
	# select the comments section of csv and make sure it does not have dublicates
	df_rdd = df.select("body").distinct().rdd.map(lambda r : r[0]) # to make the spark treat as the column type than as a row

	

	def ngrams_mapper(line):
		if line is not None and len(line.split()) >= N :
			line_ngram = nltk_ngrams(line.split(),N)
			for ngram in line_ngram:
				 return (ngram,1)
		else:
			return ("This comment lenth cannot be converted", 1)
	
	def ngrams_reducer(key1,key2):
		return key1+key2

	
	# produces result after mapping and reducing
	SentimentResultsRDD =  df_rdd.map (ngrams_mapper).reduceByKey(ngrams_reducer)

	print (SentimentResultsRDD.take(10))
	# print the characters in alphabetical order
	SentimentResults = SentimentResultsRDD.collectAsMap()
	Sorted_SentimentResults = sorted(SentimentResults.items(), key = lambda kv: kv[1],reverse=True)
	
	#store the ouput results in the text file
	with open (os.path.join(savepath,list_of_outputs[i]+outputformat), "w+") as file:
		if SentimentResults is not None:
			for elem in Sorted_SentimentResults:
				file.write(str(elem[0]) + "  " + str(elem[1]) +'\n')
	if season_totals == {}:
		season_totals = SentimentResults
	else:
		for key in SentimentResults:
			if key in season_totals:
				season_totals[key] += SentimentResults[key]
			else:
				season_totals[key] = SentimentResults[key]

		

	

    
# open output file for season stats
with open(os.path.join(savepath,"ngrams_season" +outputformat), "w+") as file:
	for elem in season_totals:
		file.write(str(elem[0]) + "  " + str(elem[1]) +'\n')
	





