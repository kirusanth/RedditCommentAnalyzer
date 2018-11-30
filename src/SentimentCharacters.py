import nltk
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
import sys
import requests
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
nltk.download ('vader_lexicon')

# create spark configuration
conf = SparkConf()
conf.setAppName("SentimentCharacters")
# create spark context with the above configuration
sc = SparkContext(conf=conf)

spark =  SparkSession.builder \
	.config(conf=sc.getConf()) \
	.getOrCreate()

episode_csv= "episode2.csv"
path= "../GameofThrones/"+ episode_csv


df = spark.read.format("csv") \
	.option("header", "true") \
	.option("inferSchema","true") \
	.load(path)


character_collection = ["jaime" ,"Cersei" , "daenerys" , "jon Snow", "sansa","arya","theon","bran","the hound","tyrion", "littlefinger","melisandre","bronn" ,"varys","tormund","gilly" ,"missandei","davos" ,"sam"]



df_rdd = df.select("body").rdd.map(lambda r : r[0])

sia =SIA()

def character_finder (line) :
	for character in character_collection:
		if character in line.lower():
			return character

# produces sentiment analyis for the line contains the characters
def sentiment_mapper(line) :
	if line is not None and any( character in line.lower() for character in character_collection):
		return (character_finder(line),(sia.polarity_scores(line),1))
	else:
		line = ""
		return ("null",(sia.polarity_scores(line),1))

#reduces sentiment analyis results
def sentiment_reducer(key1, key2):
	a=key1[0]
	b=key2[0]
	ab_dict=dict()
	ab_dict["compound"] = a['compound'] +  b['compound']
	ab_dict["pos"]=a['pos'] +  b['pos']
	ab_dict["neg"] =a['neg'] +  b['neg']
	ab_dict["neu"] =a['neu'] +  b['neu']	
	return (ab_dict ,key1[1] + key2[1] )

SentimentResultsRDD =  df_rdd.map (sentiment_mapper).reduceByKey(sentiment_reducer)

SentimentResults = SentimentResultsRDD.collect()

with open ("SentimentResults.txt","w+") as file:
	for k,v in SentimentResults:
		if k != 'null':
			compound=v[0]["compound"]/v[1]
			pos=v[0]["pos"]/v[1]
			neg=v[0]["neg"]/v[1]
			neu=v[0]["neu"]/v[1]
			file.write(str(k) + " " + str(pos)+" " + str(neg) + " " + str(neu) + " " +str(compound) +"\n")




