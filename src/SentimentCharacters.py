
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
import sys
import requests
import re

# create spark configuration
conf = SparkConf()
conf.setAppName("SentimentCharacters")
# create spark context with the above configuration
sc = SparkContext(conf=conf)

spark =  SparkSession.builder \
	.config(conf=sc.getConf()) \
	.getOrCreate()

episode_csv= "episode1.csv"
path= "../GameofThrones/"+ episode_csv


df = spark.read.format("csv") \
	.option("header", "true") \
	.option("inferSchema","true") \
	.load(path)
df.printSchema()