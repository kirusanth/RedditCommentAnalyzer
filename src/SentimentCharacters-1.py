import nltk
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
import sys
import requests
import re
import os
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
nltk.download ('vader_lexicon')

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
savepath ="../output/Text/SentimentCharacters/"

# list of input files
list_of_inputs = [episode1, episode2, episode3, episode4, episode5, episode6, episode7]

# list of outputfiles
list_of_outputs = ["character_sentiment_E1.txt", "character_sentiment_E2.txt", "character_sentiment_E3.txt", "character_sentiment_E4.txt", "character_sentiment_E5.txt", "character_sentiment_E6.txt", "character_sentiment_E7.txt"]

#character can be identified without aliases references
characters = ["cersei","theon", "bran", "hound", "melisandre", "bronn", "tormund","gilly","missandei"]
# Daenerys's aliases
daenerys = ["daenerys","stormborn","dany","khaleesi" ,"mhysa","silver lady","dragonmother","dragon queen"]
#Tyrion's aliases
tyrion = ["tyrion","imp" , "halfman","yollo","hugor hill"]
#Jamie's aliases
jamie = ["jamie","kingslayer","young lion"]
#Jon snow's aliases
jon = ["jon","snow"]
#Petyr's Baelis's aliases
petyr = ["littlefinger","petyr","baelish"]
#Davos's aliases
davos = ["davos","seaworth","onion knight","onion"]
#sansa's aliases
sansa = ["sansa","little dove","alayne stone","jonquil"]
#arya's aliases
arya = ["arya"]
#varys's aliases
varys = ["spider","rugen","eunuch"]
#melisandre's aliases
melisandre = ["melisandre","red priestess","red woman"]
#samwell's alias
sam=["sam","piggy"]


# It converts the nicknames to real name
def remove_aliases(character):
	if character in characters:
		return character
	if character in daenerys:
		return "daenerys"
	if character in tyrion:
		return "tyrion"
	if character in jamie:
		return "jamie"
	if character in jon:
		return "jon"
	if character in petyr:
		return "petyr"
	if character in davos:
		return "davos"
	if character in sansa:
		return "sansa"
	if character in arya:
		return "arya"
	if character in varys:
		return "varys"
	if character in melisandre:
		return "melisandre"
	if character in sam:
		return "sam"

# All the characters we want to look into
characters_collection = characters + daenerys + tyrion + jamie + jon + petyr + davos + arya + varys +melisandre + sam

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
	#sentiment polarity variable initialization
	sia =SIA()

	# Checks character exists in line and if so, it returns the character
	def character_finder (line) :
		for character in characters_collection: # goes through all the characters in the list
			if character in line.lower():
				character = remove_aliases(character) # combine nicknames and represent it by real name
				return character

	# produces sentiment analyis for the line contains the characters
	def sentiment_mapper(line) :
		if line is not None and any( character in line.lower() for character in characters_collection):
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
	# produces result after mapping and reducing
	SentimentResultsRDD =  df_rdd.map (sentiment_mapper).reduceByKey(sentiment_reducer)
	# print the characters in alphabetical order
	SentimentResults = sorted(SentimentResultsRDD.collect(), key= lambda x : x[0])


	outputofepisode =list()
	#store the ouput results in the text file
	with open (os.path.join(savepath,list_of_outputs[i]), "w+") as file:
		file.write("character" + ", "  + "Positive"+", " + "Negative" + ", " + "Neutral" +"\n")
		for k,v in SentimentResults:
			if k != 'null':
				# divide each value of polarity scores of character by number of occurance of the character
				compound=v[0]["compound"]/v[1]  # v[1] is the counter that tracked how many times the character has appeared
				pos=v[0]["pos"]/v[1]
				neg=v[0]["neg"]/v[1]
				neu=v[0]["neu"]/v[1]
				file.write(str(k) + ", " + str(pos)+", " + str(neg) + ", " + str(neu) + "\n")

	# adding the results of season overall stats
	if season_totals =={} :
		season_totals = SentimentResults
	else :
		for k,v in SentimentResults:
			if k == season_totals[0]:
				 season_totals[1][0]["compound"] += v[0]["compound"]
				 season_totals[1][0]["pos"] += v[0]["pos"]
				 season_totals[1][0]["neg"] += v[0]["neg"]
				 season_totals[1][0]["neu"] += v[0]["neu"]
				 season_totals[1][0]["compound"] += v[0]["compound"]
				 season_totals[1][1] += v[1]

    
# open output file for season stats
with open(os.path.join(savepath,"character_sentiment_season.txt"), "w+") as file:
	file.write("character" + ", "  + "Positive"+", " + "Negative" + ", " + "Neutral" +"\n")
	for k,v in season_totals:
		if k != 'null':
			# divide each value of polarity scores of character by number of occurance of the character
			compound=v[0]["compound"]/v[1]  # v[1] is the counter that tracked how many times the character has appeared
			pos=v[0]["pos"]/v[1]
			neg=v[0]["neg"]/v[1]
			neu=v[0]["neu"]/v[1]
			file.write(str(k) + ", " + str(pos)+", " + str(neg) + ", " + str(neu) + "\n")
