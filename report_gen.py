"""
Run this script for each day of information in the database.
"""

import pandas as pd
import code
import json

path = "message_service_message_y2018d311.csv"

df = pd.read_csv(path)
percent_precision = 4

channel_days = []


class ChannelDay:
    def __init__(self, channel_id):
        channel_days.append(self)
        self.channel_id = channel_id

        self.unique_posters = 0
        self.total_messages = 0
        self.est_percentage_of_questions = 0.0
        self.posters_pie_chart = {}

    def __str__(self):
        return f"Channel {self.channel_id}"


# Find number of unique posters in each channel
channel_ids = df["channel_id"].values
channel_ids_unique = set(channel_ids)


counter = 0
## For each day and channel id, find the number of unique posters
for channel_id in channel_ids_unique:
    new_channel_day = ChannelDay(channel_id)

    channel_rows = df.loc[df["channel_id"] == channel_id]
    
    user_ids = channel_rows["user_id"].values
    user_ids_unique = set(user_ids)
    
    total_unique_users = len(user_ids_unique)
    new_channel_day.unique_posters = total_unique_users

    # Find the number of messages per channel per day
    new_channel_day.total_messages = len(channel_rows)

    # Find estimated percentage of questions
    question_rows = channel_rows.loc[channel_rows["message_text"].str.contains("\?")]
    no_of_questions = len(question_rows)
    new_channel_day.est_percentage_of_questions = round(no_of_questions / new_channel_day.total_messages, percent_precision)

    # Make a "pie chart" of the posters
    for uuid in user_ids_unique:
        user_rows = channel_rows.loc[channel_rows["user_id"] == uuid]

        new_channel_day.posters_pie_chart[int(uuid)] = round(len(user_rows) / len(channel_rows), percent_precision)

    new_channel_day.posters_pie_chart = json.dumps(new_channel_day.posters_pie_chart)
    counter += 1
    if counter > 10:
        break

d = channel_days[0]
channel_day_series = pd.DataFrame(data=d.__dict__, index=[1])
channel_day_series.to_csv("info/test_format.csv", encoding="utf-8", index=False)

frames = []
for cd in channel_days:
    frames.append(pd.DataFrame(data=cd.__dict__, index=[1]))

code.interact(local=locals())
