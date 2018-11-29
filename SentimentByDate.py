
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
from nltk.sentiment import SentimentIntensityAnalyzer
vader_analyzer = SentimentIntensityAnalyzer()
from datetime import datetime

import sys
import requests
import re

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)

spark =  SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

episode_csv= "episode1.csv"
path= "../GameofThrones/"+ episode_csv


df = spark.read.format("csv").option("header", "true").load(path)

rdd1 = df.select("created_utc","body").rdd

def getsentiment(x):
	
	text = vader_analyzer.polarity_scores(x[1]) 
	neg = text['neg']
	neu = text['neu']
	pos = text['pos']
	if(neg > neu and neg > pos):
		sentScore = (1,0,0)
	elif (neu > neg and neu > pos):
		sentScore = (0,1,0)
	elif (pos > neg and pos > neu):
		sentScore = (0,0,1)
	else:
		sentScore = (0,0,0)
	d=""
	if re.match(r"^\d+?$", str(x[0])):
		d = datetime.fromtimestamp(float(x[0])).strftime('"%b-%d"')
	# else:
	# 	d = datetime.fromtimestamp(0000000000).strftime('"%m%d"')	
	# time = 	

	return (d, sentScore)


timerdd = rdd1.map(getsentiment)


print(timerdd.take(100))

