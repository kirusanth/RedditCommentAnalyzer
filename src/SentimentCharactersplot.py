import matplotlib.pyplot as plot;
import matplotlib.pyplot as plot
import numpy as np
import seaborn as sns
import os
 
# set width of bar
barWidth = 0.17
 

pos =dict()
neg=dict()
neu =dict()
#This will add pos, neg, and neu values to list which it gets from the line it splits
def splitter (line):
	splited_line = line.split(", ")
	pos[splited_line[0]] = float(splited_line[1])
	neg[splited_line[0]] = float(splited_line[2])
	neu[splited_line[0]] = float(splited_line[3])
	return (pos,neg,neu)

def sortedkeysandvalues(dictionary):
	dictionary = sorted(dictionary.items(),key=lambda kv: kv[1])
	keys=list()
	values=list()
	for k, v in dictionary:
		keys.append(k)
		values.append(v)
	return (keys,values)
#paths for input and output (directory)
inputpath = "../output/Text/SentimentCharacters/"
outputpath = "../output/Image/SentimentCharacters/"

list_of_inputs = ["character_sentiment_E1.txt", "character_sentiment_E2.txt", "character_sentiment_E3.txt", "character_sentiment_E4.txt", "character_sentiment_E5.txt", "character_sentiment_E6.txt", "character_sentiment_E7.txt", "character_sentiment_season.txt"]
list_of_outputs = ["character_sentiment_E1", "character_sentiment_E2", "character_sentiment_E3", "character_sentiment_E4", "character_sentiment_E5", "character_sentiment_E6", "character_sentiment_E7", "character_sentiment_season"]
for episode_nr in range(len(list_of_inputs)):

	with open(os.path.join(inputpath,list_of_inputs[episode_nr]), "r") as file:
		data = file.readlines()
		positive = dict()
		negative = dict()
		neutral = dict()
		for i in data[1:] :
			pos,neg,neu = splitter(i)
			positive.update(pos)
			negative.update(neg)
			neutral.update(neu)

		poskeys,posvalues = sortedkeysandvalues(positive)
		negkeys,negvalues = sortedkeysandvalues(negative)
		neukeys,negvalues = sortedkeysandvalues(neutral)

	



		plot.figure(figsize=(15.0, 5.0))
		plot.subplot(1, 3, 1)
		ypos = np.arange(len(posvalues))
		plot.bar(ypos, posvalues, color=sns.color_palette("husl",20), align = 'center', alpha = 0.7)
		plot.xticks(ypos, poskeys,rotation=80)
		plot.subplots_adjust(bottom = 0.3)
		plot.ylabel("Average Positive Scores")
		plot.xlabel("Characters")
		plot.title("Characters Vs Average Positive Scores")
	
		plot.subplot(1, 3, 2)
		ypos = np.arange(len(negvalues))
		plot.bar(ypos, negvalues, color=sns.color_palette("husl",20), align = 'center', alpha = 0.7)
		plot.xticks(ypos, negkeys,rotation=80)
		plot.subplots_adjust(bottom = 0.3)
		plot.ylabel("Average Negative Scores")
		plot.xlabel("Characters")
		plot.title("Characters Vs Average Negative Scores")
	
		plot.subplot(1, 3, 3)
		ypos = np.arange(len(negvalues))
		plot.bar(ypos, negvalues, color=sns.color_palette("husl",20), align = 'center', alpha = 0.7)
		plot.xticks(ypos, neukeys,rotation=80)
		plot.subplots_adjust(bottom = 0.3)
		plot.ylabel("Average Neutral Scores")
		plot.xlabel("Characters")
		plot.title("Characters Vs Average Neutral Scores")

		plot.savefig(os.path.join(outputpath,list_of_outputs[episode_nr]))
		episode_nr += 1

