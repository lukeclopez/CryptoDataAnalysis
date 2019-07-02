"""
Run this script for each day of information in the database.
"""

import pandas as pd
import code

path = "message_service_message_y2018d311.csv"

df = pd.read_csv(path)

channel_days = []

class ChannelDay:
    def __init__(self, id):
        channel_days.append(self)
        self.id = id
        self.unique_posters = 0
        self.total_messages = 0
        self.est_percentage_of_questions = 0.0
        self.top_posters_pie_chart = {}

    def __str__(self):
        return f"Channel {self.id}"

# Find number of unique posters in each channel
channel_ids = df["channel_id"].values
channel_ids_unique = set(channel_ids)


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
    new_channel_day.est_percentage_of_questions = no_of_questions / new_channel_day.total_messages

    # Make a "pie chart" of the top 10 posters
    user_message_percents = {}
    for uuid in user_ids_unique:
        user_rows = channel_rows.loc[channel_rows["user_id"] == uuid]

        # The AMOUNT of messages is the key, the user ID is the value.
        # I did it this way to make it easier to match amounts 
        user_message_percents[uuid] = len(user_rows) / len(channel_rows)

    new_channel_day.top_posters_pie_chart = user_message_percents

    break

    





# Find how many people post daily
## Same as number 1?

# df.to_csv("info/columns.csv", encoding="utf-8")

code.interact(local=locals())