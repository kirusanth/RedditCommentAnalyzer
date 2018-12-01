
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


episode1_csv = "episode1.csv"
episode2_csv = "episode2.csv"
episode3_csv = "episode3.csv"
episode3_2_csv = "episode3part2.csv"
episode4_csv = "episode4.csv"
episode5_csv = "episode5.csv"
episode6_csv = "episode6.csv"
episode7_csv = "episode7.csv"
episode7_2_csv = "episode7part2.csv"

path1 = "../GameofThrones/"+ episode1_csv
path2 = "../GameofThrones/"+ episode2_csv
path3 = "../GameofThrones/"+ episode3_csv
path4 = "../GameofThrones/"+ episode3_2_csv
path5 = "../GameofThrones/"+ episode4_csv
path6 = "../GameofThrones/"+ episode5_csv
path7 = "../GameofThrones/"+ episode6_csv
path8 = "../GameofThrones/"+ episode7_csv
path9 = "../GameofThrones/"+ episode7_2_csv
savepath ="../output/Text/SentimentEpisode/"


df1 = spark.read.format("csv").option("header", "true").load(path1)
df2 = spark.read.format("csv").option("header", "true").load(path2)
df3 = spark.read.format("csv").option("header", "true").load(path3)
df4 = spark.read.format("csv").option("header", "true").load(path4)
df5 = spark.read.format("csv").option("header", "true").load(path5)
df6 = spark.read.format("csv").option("header", "true").load(path6)
df7 = spark.read.format("csv").option("header", "true").load(path7)
df8 = spark.read.format("csv").option("header", "true").load(path8)
df9 = spark.read.format("csv").option("header", "true").load(path9)


rdd1 = df1.select("created_utc","body").rdd
rdd2 = df2.select("created_utc","body").rdd
rdd3 = df3.select("created_utc","body").rdd
rdd4 = df4.select("created_utc","body").rdd
rdd5 = df5.select("created_utc","body").rdd
rdd6 = df6.select("created_utc","body").rdd
rdd7 = df7.select("created_utc","body").rdd
rdd8 = df8.select("created_utc","body").rdd
rdd9 = df9.select("created_utc","body").rdd




#assing episode and sentiment based on the epoch time(created_utc)
def getEpisodeSentiment(x):

	if x[1] is not None:
		text = vader_analyzer.polarity_scores(x[1])
		if re.match(r"^\d+?$", str(x[0])) and float(x[0]) < currentTs:
			d = float(x[0])
			if d >= 1500253200 and d < 1500858000:
				e = "edpisode1"
			elif d >= 1500858000 and d <1501462800:
				e = "edpisode2"
			elif d >= 1501462800 and d <1502067600:
				e = "edpisode3"
			elif d >= 1502067600 and d <1502672400:
				e = "edpisode4"
			elif d >= 1502672400 and d <1503277200:
				e = "edpisode5"				
			elif d >= 1503277200 and d <1503882000:
				e = "edpisode6"	
			elif d >= 1503882000 and d <1504486800:
				e = "edpisode7"	
			else: 
				e = "None"
		else: 
			e = "None"

		return(e,(text,1))
	else:
		text = vader_analyzer.polarity_scores("") 
		
		return ("None",(text,1))

def getsentimentpoint(x):
	if x[1] is not None:
		d=""
		text = vader_analyzer.polarity_scores(x[1]) 
		neg = text['neg']
		neu = text['neu']
		pos = text['pos']
		if(neg > neu and neg > pos):
			text['neg'] = 1
			text['neu'] = 0
			text['pos'] = 0
		elif (neu > neg and neu > pos):
			text['neg'] = 1
			text['neu'] = 1
			text['pos'] = 0
		elif (pos > neg and pos > neu):
			text['neg'] = 0
			text['neu'] = 0
			text['pos'] = 1
		else:
			text['neg'] = 0
			text['neu'] = 0
			text['pos'] = 0
		
		if re.match(r"^\d+?$", str(x[0])) and float(x[0]) < currentTs:
			d = datetime.fromtimestamp(float(x[0])).strftime('"%y-%m-%d"')
				
		else:
			d = datetime.fromtimestamp(0000000000).strftime('"%y-%m-%d"')	
			

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


# map the each episode.
erdd1 = rdd1.map(getEpisodeSentiment)
erdd2 = rdd2.map(getEpisodeSentiment)
erdd3 = rdd3.map(getEpisodeSentiment)
erdd4 = rdd4.map(getEpisodeSentiment)
erdd5 = rdd5.map(getEpisodeSentiment)
erdd6 = rdd6.map(getEpisodeSentiment)
erdd7 = rdd7.map(getEpisodeSentiment)
erdd8 = rdd8.map(getEpisodeSentiment)
erdd9 = rdd9.map(getEpisodeSentiment)


# combined the all the rdd for episodes
rddUnion = rdd1.union(rdd2).union(rdd3).union(rdd4).union(rdd5).union(rdd6).union(rdd7).union(rdd8).union(rdd9)


# produce mapped result 
sentimentPointRdd = rddUnion.map(getsentimentpoint)

# combine the mapped episode rdds together
erddUnion = erdd1.union(erdd2).union(erdd3).union(erdd4).union(erdd5).union(erdd6).union(erdd7).union(erdd8).union(erdd9)

# produces reduced result 
byEpisodeRdd = erddUnion.reduceByKey(sentiment_reducer)
byDateRdd = sentimentPointRdd.reduceByKey(sentiment_reducer)


# print result in sorted manner
byDateresult = sorted(byDateRdd.collect(), key= lambda x : x[0])

byEpisoderesult = sorted(byEpisodeRdd.collect(), key= lambda x : x[0])


try:
    # Create target Directory
    os.mkdir(savepath)
   
except FileExistsError:
	pass
   



with open (os.path.join(savepath,"DateResult.txt"),"w+") as file:
	for k,v in byDateresult:
		if k != 'None':
			compound=v[0]["compound"]/v[1]
			pos=v[0]["pos"]
			neg=v[0]["neg"]
			neu=v[0]["neu"]
			file.write(str(k) + " " + str(pos)+" " + str(neg) + " " + str(neu) + " " +str(compound) +"\n")


with open (os.path.join(savepath,"EpisodeResults.txt"),"w+") as file:
	for k,v in byEpisoderesult:
		if k != 'None':
			compound=v[0]["compound"]/v[1]
			pos=v[0]["pos"]/v[1]
			neg=v[0]["neg"]/v[1]
			neu=v[0]["neu"]/v[1]
			file.write(str(k) + " " + str(pos)+" " + str(neg) + " " + str(neu) + " " +str(compound) +"\n")



