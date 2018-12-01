from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
import sys
import os
import requests
import re
from operator import add

# create spark configuration
conf = SparkConf()
conf.setAppName("CharacterTF")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# building session
spark = SparkSession.builder \
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

#outputpath
savepath ="../output/Text/CharacterTF/"

list_of_inputs = [episode1, episode2, episode3, episode4, episode5, episode6, episode7]
list_of_outputs = ["character_count_E1.txt", "character_count_E2.txt", "character_count_E3.txt", "character_count_E4.txt", "character_count_E5.txt", "character_count_E6.txt", "character_count_E7.txt"]

# characters we want
character_collection = ["jaime", "cersei", "dany", "jon","sansa", "arya", "theon", "bran", "hound", "tyrion", "littlefinger", "melisandre", "bronn", "varys", "tormund", "gilly", "missandei", "davos", "sam"]

i = 0
season_totals = {}
### For each episode file
for i in range(len(list_of_inputs)):

    # get file
    df = spark.read.format("csv") \
    	.option("header", "true") \
    	.option("inferSchema","true") \
    	.load(list_of_inputs[i])

    # get comments as lines
    text = df.select("body").rdd.map(lambda r : r[0]).filter(lambda x : x is not None)

    # clean data and split data into words
    processed_text = text.map(lambda line: re.sub(r'(\d|\W)+'," ", line)).flatMap(lambda line: line.lower().split(" "))

    # filter the words to get only characters
    characters = processed_text.filter(lambda w: any (e in w for e in character_collection))

    # map each character to be a pair of (character,1)
    characters_counts = characters.map(lambda x: (x, 1))

    # aggregation keys to a dict
    characters_totals = characters_counts.reduceByKey(add).collectAsMap()

    # dict sorting, return list
    characters_totals_sorted = sorted(characters_totals.items(), key = lambda kv: kv[1])

    # open output file (for each episode)
    with open(os.path.join(savepath,list_of_outputs[i]), "w+") as file:
        for elem in characters_totals_sorted:
            if elem[0] in character_collection:
                file.write(elem[0] + ' ' + str(elem[1]) + '\n')

    # add counts of every episode
    if season_totals == {}:
        season_totals = characters_totals
    else:
        for key in characters_totals:
            if key in season_totals:
                season_totals[key] += characters_totals[key]
            else:
                season_totals[key] = characters_totals[key]

    i += 1

# sort final dict
season_totals = sorted(season_totals.items(), key = lambda kv: kv[1])

# open output file (for final count)
with open(os.path.join(savepath,"character_count_season.txt"), "w+") as file:
    for elem in season_totals:
        if elem[0] in character_collection:
            file.write(elem[0] + ' ' + str(elem[1]) + '\n')
