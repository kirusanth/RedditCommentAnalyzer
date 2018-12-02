import nltk
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext, SparkSession
import sys
import requests
import re
import os
from nltk import ngrams as nltk_ngrams
from nltk.corpus import stopwords
from nltk import word_tokenize

#preprocess variables
nltk.download('stopwords')
nltk.download('punkt')
nl_stop_words =  set(stopwords.words('english'))
pattern = re.compile("a-z")
our_stop_words = ["like","the", "think", "yeah", "its","that","you", "well","thats","this","that", "and", "what","they","she","know","but","thought", "would","yes","really","not","hes"]

#size of N gram
N = 10

#limit of records for files
recordlimit = 20 # top 20 records per file

# input files
episode1 = "../GameofThrones/episode1.csv" 
episode2 = "../GameofThrones/episode2.csv" 
episode3 = "../GameofThrones/episode3full.csv" 
episode4 = "../GameofThrones/episode4.csv" 
episode5 = "../GameofThrones/episode5.csv" 
episode6 = "../GameofThrones/episode6.csv" 
episode7 = "../GameofThrones/episode7full.csv"

# output path
savepath ="../output/Text/Ngrams/10grams/"

# list of input files
list_of_inputs = [episode1, episode2, episode3, episode4, episode5, episode6, episode7]

# list of outputfiles
list_of_outputs = ["ngrams_E1", "ngrams_E2", "ngrams_E3", "ngrams_E4", "ngrams_E5", "ngrams_E6", "ngrams_E7"]
# output file format
outputformat = ".txt" #textfile

# create spark configuration
conf = SparkConf()
conf.setAppName("SentimentCharacters")
# create spark context with the above configuration
sc = SparkContext(conf=conf)

# Create spark session 
spark =  SparkSession.builder \
	.config(conf=sc.getConf()) \
	.getOrCreate()


#function to clean data
def regex_clean(word):
    #lower case all words
    word = word.lower()
    #remove numbers and special characters
    word = re.sub (r'(\d|\W)+',"", word)
    #remove words with 2 or less length
    word = re.sub(r'\b\w{1,2}\b', '', word)
    return word
#preprocessor
def ngram_mapper_preprocessing (line):
	prprocessed_comment = "" # this produce a line without stopwords
	# stores the stop words
	if line is not  None:
		for word in line.split() :
			if word not in nl_stop_words: # remove the stop words in english
				word = regex_clean(word) # remove special characters, word has less than 2 characters, and numbers
				if word not in our_stop_words: # remove most common words that we find occur more than 100000 times
					prprocessed_comment += " "+ word
		return prprocessed_comment
	else:
		return line # line is none (dealt in the mapper)
#mapper helper
def ngrams_mapper(line):
	preprocessed_comment = ngram_mapper_preprocessing (line) # preprocess line

	#check whether line is not empty and size is at least equal to number of ngram
	if preprocessed_comment is not None and len(word_tokenize(preprocessed_comment)) >= N :
		line_ngram = nltk_ngrams(word_tokenize(preprocessed_comment),N) # this produces list contains tuples that has ngrams words
		for ngram in line_ngram: 
			comment ="" #add the word in the tuple to make it look like string that list of words
			for word in ngram:
				comment += " "+ word
			return (comment,1)
	else:
		return ("The length of the comment is less for ngrams conversion", 1)

#reducer helper	
def ngrams_reducer(key1,key2):
	return key1+key2
#season stats
season_totals = dict()
### For each episode file
for i in range(len(list_of_inputs)):
	# read the files in csv format
	df = spark.read.format("csv") \
		.option("header", "true") \
		.option("inferSchema","true") \
		.load(list_of_inputs[i])
	# select the comments section of csv and make sure it does not have dublicates
	df_rdd = df.select("body").distinct().rdd.map(lambda r : r[0]) # to make the spark treat as the column type than as a row
	
	# produces result after mapping and reducing
	ngramsRDD =  df_rdd.map (ngrams_mapper).reduceByKey(ngrams_reducer)

	# print the characters in alphabetical order
	ngrams_results = ngramsRDD.collectAsMap()
	Sorted_ngrams_results = sorted(ngrams_results.items(), key = lambda kv: kv[1],reverse=True)
	
	#store the ouput results in the text file
	with open (os.path.join(savepath,list_of_outputs[i]+outputformat), "w+") as file:
		if ngrams_results is not None:
			limit=0 # stop the write to file ( only writer 20 ngrams to file)
			for elem in Sorted_ngrams_results:
				limit +=1
				if limit <=recordlimit : # only record 20 records in file
					file.write(str(elem[0]) + "  " + str(elem[1]) +'\n')
				#add it to the final season stats
				if season_totals == {}:
					season_totals[elem[0]] = elem[1]
				else:
					if elem[0] in season_totals:
						season_totals[elem[0]] += elem[1]
					else:
						season_totals[elem[0]] = elem[1]
	    
# open output file for season stats
with open(os.path.join(savepath,"ngrams_season" +outputformat), "w+") as file:
	Sorted_season_totals = sorted(season_totals.items(), key = lambda kv: kv[1],reverse=True)
	limit=0 # stop the write to file ( only writer 20 ngrams to file)
	for k,v in Sorted_season_totals:
		file.write(str(k) + " " + str(v)+'\n')
		limit +=1
		if limit == recordlimit:
			break
	





