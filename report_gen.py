"""
Run this script for each day of information in the database.
"""

import pandas as pd
import code

path = "message_service_message_y2018d311.csv"

df = pd.read_csv(path)

# Find number of unique posters in each channel
channel_ids = df["channel_id"].values
channel_ids_unique = set(channel_ids)

user_ids_unique = 5

## For each day and channel id, find the number of unique posters
for channel_id in channel_ids_unique:
    channel_rows = df.loc[df["channel_id"] == channel_id]
    user_ids = channel_rows["user_id"].values
    user_ids_unique = set(user_ids)
    total_unique_users = len(user_ids_unique)
    print(f"Channel: {channel_id} Unique Posters: {total_unique_users}")



# Find how many people post daily
## Same as number 1?

# Find the number of messages per channel per day
no_of_messages = len(df)


# Find estimated percentage of questions
question_rows = df.loc[df["message_text"].str.contains("\?")]
no_of_questions = len(question_rows)
est_percentage_of_questions = no_of_questions / no_of_messages


# Get the top 10 posters (across days) and find what percentage of messages they are responsible for.
## Get the top 10 for each day, then find the names (or ids) that appear most frequently in 7 and 30-day periods


# df.to_csv("info/columns.csv", encoding="utf-8")

code.interact(local=locals())