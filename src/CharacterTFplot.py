import matplotlib.pyplot as plot;
import matplotlib.pyplot as plot
import numpy as np
import seaborn as sns
import os

i = 0
episode_nr = 0
character = ()
counts = ()

#paths for input and output (directory)
inputpath = "../output/Text/CharacterTF/"
outputpath = "../output/Image/CharacterTF/"

list_of_inputs = ["character_count_E1.txt", "character_count_E2.txt", "character_count_E3.txt", "character_count_E4.txt", "character_count_E5.txt", "character_count_E6.txt", "character_count_E7.txt", "character_count_season.txt"]
list_of_outputs = ["character_occurence_E1", "character_occurence_E2", "character_occurence_E3", "character_occurence_E4", "character_occurence_E5", "character_occurence_E6", "character_occurence_E7", "character_occurence_season" ]
# plot graph for each episode and all episodes
for episode_nr in range(len(list_of_inputs)):
    # open files
    with open(os.path.join(inputpath,list_of_inputs[episode_nr]), "r") as file:
        # create list (each element is a line)
        data = file.readlines()
        # add to tuples the character and the number of occurrences
        for i in range(len(data)):
            split_data = data[i].split()
            character += (split_data[0],)
            counts += (int(split_data[1]),)

    # plot graph
    ypos = np.arange(len(character))
    plot.figure()
    plot.bar(ypos, counts, color=sns.color_palette("hls",20), align = 'center', alpha = 0.6)
    plot.xticks(ypos, character,rotation=80)
    plot.subplots_adjust(bottom = 0.3)
    plot.ylabel("Occurrences")
    plot.xlabel("Characters")
    plot.title("Characters Vs Occurrences")
  
    plot.savefig(os.path.join(outputpath,list_of_outputs[episode_nr]))
    episode_nr += 1
    character = ()
    counts = ()
