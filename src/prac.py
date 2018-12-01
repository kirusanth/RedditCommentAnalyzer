
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
from nltk.sentiment import SentimentIntensityAnalyzer
vader_analyzer = SentimentIntensityAnalyzer()
from datetime import datetime

import sys
import requests
import re
import os

# create spark configuration
conf = SparkConf()
conf.setAppName("StentimentByDate")
# create spark context with the above configuration
sc = SparkContext(conf=conf)

spark =  SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

currentTs = datetime.now().timestamp()


# episode1_csv = "episode1.csv"
# episode2_csv = "episode2.csv"
# episode3_csv = "episode3.csv"
# episode3_2_csv = "episode3part2.csv"
# episode4_csv = "episode4.csv"
# episode5_csv = "episode5.csv"
# episode6_csv = "episode6.csv"
# episode7_csv = "episode7.csv"
# episode7_2_csv = "episode7part2.csv"

# path1 = "../GameofThrones/"+ episode1_csv
# path2 = "../GameofThrones/"+ episode2_csv
# path3 = "../GameofThrones/"+ episode3_csv
# path4 = "../GameofThrones/"+ episode3_2_csv
# path5 = "../GameofThrones/"+ episode4_csv
# path6 = "../GameofThrones/"+ episode5_csv
# path7 = "../GameofThrones/"+ episode6_csv
# path8 = "../GameofThrones/"+ episode7_csv
# path9 = "../GameofThrones/"+ episode7_2_csv

# df1 = spark.read.format("csv").option("header", "true").load(path1)
# df2 = spark.read.format("csv").option("header", "true").load(path2)
# df3 = spark.read.format("csv").option("header", "true").load(path3)
# df4 = spark.read.format("csv").option("header", "true").load(path4)
# df5 = spark.read.format("csv").option("header", "true").load(path5)
# df6 = spark.read.format("csv").option("header", "true").load(path6)
# df7 = spark.read.format("csv").option("header", "true").load(path7)
# df8 = spark.read.format("csv").option("header", "true").load(path8)
# df9 = spark.read.format("csv").option("header", "true").load(path9)


# rdd1 = df1.select("created_utc","body").rdd
# rdd2 = df2.select("created_utc","body").rdd
# rdd3 = df3.select("created_utc","body").rdd
# rdd4 = df4.select("created_utc","body").rdd
# rdd5 = df5.select("created_utc","body").rdd
# rdd6 = df6.select("created_utc","body").rdd
# rdd7 = df7.select("created_utc","body").rdd
# rdd8 = df8.select("created_utc","body").rdd
# rdd9 = df9.select("created_utc","body").rdd

path = "../GameofThrones/"
filelist = os.listdir(path)
num = 1;
for file in filelist:
	if file.endswith(".csv"):
		try:
			df[num] = spark.read.format("csv").option("header", "true").load(path+file)
			rdd[num] = df[num].select("created_utc","body").rdd
		except:
			pass
# rddUnion = rdd1.union(rdd2).union(rdd3).union(rdd4).union(rdd5).union(rdd6).union(rdd7).union(rdd8).union(rdd9)







def getsentimentpoint(x):
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
		if re.match(r"^\d+?$", str(x[0])) and float(x[0]) < currentTs:
			d = datetime.fromtimestamp(float(x[0])).strftime('"%y-%b-%d"')
				
		else:
			d = datetime.fromtimestamp(0000000000).strftime('"%y-%b-%d"')	
			

		return (d, (text,1))
	else:
		
		text = vader_analyzer.polarity_scores("") 
		return ("None",(text,1))

def sentiment_reducer(key1, key2):

	a = key1[0]
	b = key2[0]
	sumScore=dict()
	sumScore['compound'] = a['compound'] +  b['compound']
	sumScore['neg'] = a['neg'] + b['neg']
	sumScore['neu'] = a['neu'] + b['neu']
	sumScore['pos'] = a['pos'] + b['pos']
	return (sumScore,key1[1]+key2[1]) 


# rddUnion = rdd1.uinon(rdd2).union(rdd3).union(rdd4).union(rdd5).union(rdd6).union(rdd7).union(rdd8).union(rdd9)


sentimentPointRdd = rdd[1].map(getsentimentpoint)


byDateRdd = sentimentPointRdd.reduceByKey(sentiment_reducer)



byDateresult = byDateRdd.collect()

with open ("DateResults.txt","w+") as file:
	for k,v in byDateresult:
		if k != 'None':
			compound=v[0]["compound"]/v[1]
			pos=v[0]["pos"]
			neg=v[0]["neg"]
			neu=v[0]["neu"]
			file.write(str(k) + " " + str(pos)+" " + str(neg) + " " + str(neu) + " " +str(compound) +"\n")

