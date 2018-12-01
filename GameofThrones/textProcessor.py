
import re
import pandas as pd

testepisode_csv = "testepisode.csv"
episode1_csv = "episode1.csv"
episode2_csv = "episode2.csv"
episode3_part1_csv = "episode3p1.csv"
episode3_part2_csv = "episode3p2.csv"
episode4_csv = "episode4.csv"
episode5_csv = "episode5.csv"
episode6_csv = "episode6.csv"
episode7_part1_csv = "episode7p1.csv"
episode7_part2_csv = "episode7p2.csv"

df1 = pd.read_csv(episode3_part1_csv)
df2 = pd.read_csv(episode3_part2_csv)
frames = [df1, df2]
full_episode3 = pd.concat(frames).drop_duplicates()

full_episode3.to_csv("../GameofThrones/episode3full.csv")

df1 = pd.read_csv(episode7_part1_csv)
df2 = pd.read_csv(episode7_part2_csv)
frames = [df1, df2]
full_episode7 = pd.concat(frames).drop_duplicates()

full_episode7.to_csv("../GameofThrones/episode7full.csv")