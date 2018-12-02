import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import re
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os

savepath ="../output/Text/SentimentEpisode/"
outputpath = "../output/Image/SentimentEpisode/"
byDatefile = savepath + "DateResult.txt"
byEposidefile = savepath + "EpisodeResults.txt"


date=list()
neg=list()
neu=list()
pos=list()
com=list()
line_num = 0




byDate = open(byDatefile,"r")
byEpisode = open(byEposidefile,"r")

for line in byDate.read().split("\n"):
	
	if line is not None:

		
		words = line.split(",")
		if len(words) > 4:
			matches = re.findall('\"17-(\d{2}-\d{2})\"',words[0])
			if matches:
				d = re.search('\"\d{2}-(\d{2}-\d{2})\"',words[0])
				date.insert(line_num,d.group(1))
				pos.insert(line_num,words[1])
				neg.insert(line_num,words[2])
				neu.insert(line_num,words[3])
				com.insert(line_num,words[4])
			line_num += 1


neg = [None if v is '' else float(v) for v in neg]
neu = [None if v is '' else float(v) for v in neu]
pos = [None if v is '' else float(v) for v in pos]

fig, ax1 = plt.subplots()




y_pos = np.arange(len(neg))  # the x locations for the groups
width = 0.35       # the width of the bars

color = 'tab:red'
ax1.set_xticks(y_pos + width / 2)
ax1.set_xticklabels(date, rotation=90)
ax1.set_ylabel('Neutral', color=color)
ax1.plot(date, neu, color=color)
ax1.tick_params(axis='y', labelcolor=color)

ax = ax1.twinx()  # instantiate a second axes that shares the same x-axis


fig.tight_layout()  # otherwise the right y-label is slightly clipped

# fig = plt.figure()

rects1 = ax.bar(y_pos, neg, width, color='royalblue')


rects2 = ax.bar(y_pos+width, pos, width, color='seagreen')

# add some
ax.set_ylabel('Negative/Positive')
ax.set_title('number of neg/neu/pos comments per day')


red_patch = mpatches.Patch(color='red', label='Neutral')
blue_patch = mpatches.Patch(color='royalblue', label='Negative')
green_patch = mpatches.Patch(color='seagreen', label='positive')
plt.legend(handles=[red_patch,green_patch,blue_patch])
plt.xticks(rotation=90)
plt.show()
# plt.savefig("SentimentByDayPlot")
plt.savefig(os.path.join(outputpath,"SentimentByDayPlot"))

episode=list()
neg=list()
neu=list()
pos=list()
com=list()
line_num = 0


for line in byEpisode.read().split("\n"):
	
	if line is not None:

		
		words = line.split(",")
		if len(words) > 4:
			
			episode.insert(line_num,words[0])
			pos.insert(line_num,words[1])
			neg.insert(line_num,words[2])
			neu.insert(line_num,words[3])
			com.insert(line_num,words[4])
		line_num += 1




neg = [None if v is '' else float(v) for v in neg]
neu = [None if v is '' else float(v) for v in neu]
pos = [None if v is '' else float(v) for v in pos]

fig, ax1 = plt.subplots()




y_pos = np.arange(len(neg))  # the x locations for the groups
width = 0.35       # the width of the bars

ax1.set_title('number of neg/neu/pos comments per day')
ax1.set_xticks(y_pos + width / 2)
ax1.set_xticklabels(episode, rotation=90)
ax1.set_ylabel('Sentiment Average')
ax1.plot(episode, neu, color="red")
ax1.tick_params(axis='y')

# ax1 = ax.twinx()  # instantiate a second axes that shares the same x-axis
# ax2 = ax.twinx() 

fig.tight_layout()  # otherwise the right y-label is slightly clipped

# fig = plt.figure()




red_patch = mpatches.Patch(color='red', label='Neutral')
blue_patch = mpatches.Patch(color='royalblue', label='Negative')
green_patch = mpatches.Patch(color='seagreen', label='positive')


# add some
# ax1.set_ylabel('Negative', color="green")
ax1.plot(episode,neg,color="royalblue")
# ax1.set_title('number of neg/neu/pos comments per day')
# ax2.set_ylabel('Positive', color="blue")
ax1.plot(episode,pos,color="seagreen")


# ax.legend( (rects1[0], rects2[0]), ('neg', 'pos') )
plt.legend(handles=[red_patch,green_patch,blue_patch])
plt.xticks(rotation=90)
plt.show()
plt.savefig(os.path.join(outputpath,"SentimentByEpisodePlot"))


