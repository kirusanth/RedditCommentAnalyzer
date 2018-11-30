
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
	if x[1] is not None:
		text = vader_analyzer.polarity_scores(x[1]) 
		neg = text['neg']
		neu = text['neu']
		pos = text['pos']
		if(neg > neu and neg > pos):
			# sentScore = {'neg':1,'neu':0,'pos':0}
			text['neg'] = 1
			text['neu'] = 0
			text['pos'] = 0
		elif (neu > neg and neu > pos):
			# sentScore = {'neg':0,'neu':1,'pos':0}
			text['neg'] = 1
			text['neu'] = 1
			text['pos'] = 0
		elif (pos > neg and pos > neu):
			# sentScore = {'neg':0,'neu':0,'pos':1}
			text['neg'] = 0
			text['neu'] = 0
			text['pos'] = 1
		else:
			# sentScore = {'neg':0,'neu':0,'pos':0}
			text['neg'] = 0
			text['neu'] = 0
			text['pos'] = 0
		d=""
		if re.match(r"^\d+?$", str(x[0])):
			d = datetime.fromtimestamp(float(x[0])).strftime('"%b-%d"')
		# else:
		# 	d = datetime.fromtimestamp(0000000000).strftime('"%m%d"')	
		# time = 	

		return (d, (text,1))
	else:
		
		text = vader_analyzer.polarity_scores("") 
		return ("null",(text,1))

def sentiment_reducer(key1, key2):

	a = key1[0]
	b = key2[0]
	sumScore=dict()
	sumScore['compound'] = a['compound'] +  b['compound']
	sumScore['neg'] = a['neg'] + b['neg']
	sumScore['neu'] = a['neu'] + b['neu']
	sumScore['pos'] = a['pos'] + b['pos']
	return (sumScore,key1[1]+key2[1]) 



timerdd = rdd1.map(getsentiment)

print(timerdd.take(10))
resultRdd = timerdd.reduceByKey(sentiment_reducer)
print(resultRdd.take(10))


result = resultRdd.collect()

with open ("DateResults.txt","w+") as file:
	for k,v in result:
		if k != 'null':
			compound=v[0]["compound"]/v[1]
			pos=v[0]["pos"]
			neg=v[0]["neg"]
			neu=v[0]["neu"]
			file.write(str(k) + " " + str(pos)+" " + str(neg) + " " + str(neu) + " " +str(compound) +"\n")

