import re
import os
import pandas as pd
import numpy as np


# The path where we store input files
input_path = "../GameofThrones/"

# input csv files names
episode1_csv = "episode1.csv"
episode2_csv = "episode2.csv"
episode3_part1_csv = "episode3p1.csv"
episode3_part2_csv = "episode3p2.csv"
episode4_csv = "episode4.csv"
episode5_csv = "episode5.csv"
episode6_csv = "episode6.csv"
episode7_part1_csv = "episode7p1.csv"
episode7_part2_csv = "episode7p2.csv"

# join dataset of episodes that are in parts
def joiner(csv1,csv2) :
	df1 = pd.read_csv(os.path.join(input_path,csv1))
	df2 = pd.read_csv(os.path.join(input_path,csv2))
	frames = [df1, df2]
	full_episode = pd.concat(frames).drop_duplicates() # delete duplicates
	return full_episode

def main():
	episode3 = joiner(episode3_part1_csv,episode3_part2_csv)
	episode3.to_csv(os.path.join(input_path,"episode3full.csv"))
	episode7 = joiner(episode7_part1_csv,episode7_part2_csv)
	episode7.to_csv(os.path.join(input_path,"episode7full.csv"))


if __name__ == "__main__":
	main()





