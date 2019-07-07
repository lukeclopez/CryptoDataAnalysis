import pandas as pd
import code
import json
import pathlib as pl
import sqlalchemy
import funcs
import os
from pprint import pprint

HOST = "ca-pro-rds-prod.cllw6gj7ocf7.us-east-1.rds.amazonaws.com"
DB_NAME = "telegram_data"
DB_USER =  "analytics"
DB_PASS = "acaa301cf8214359b9bb9722b3aa469f"

engine = sqlalchemy.create_engine(f"postgres://{DB_USER}:{DB_PASS}@{HOST}:5432/{DB_NAME}")

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


tables_path = pl.Path("info/relevant_tables.txt")
csv_folder = pl.Path.cwd() / "info" / "csv"

# each_day_dfs = funcs.create_df_for_each_relevant_table(tables_path, engine, limit=30)
each_day_csvs = [f for f in os.listdir(csv_folder)]
each_day_dfs = []

for csv_file in each_day_csvs:
    new_df = pd.read_csv(csv_folder / csv_file)
    each_day_dfs.append(new_df)
    break


def analyze_daily_data(data_source):
    for df in data_source:
        channel_ids = funcs.get_unique_channels(df)
        channel_dfs = funcs.get_channel_dfs_list(df, channel_ids)

        for channel_df in channel_dfs:
            channel_id = funcs.get_channel_id(channel_df)
            channel_unique_users = funcs.get_unique_posters_list(channel_df)

            channel_day = ChannelDay(channel_id)

            channel_day.unique_posters = funcs.get_unique_posters_count(channel_df)
            channel_day.total_messages = funcs.get_message_count(channel_df)
            channel_day.est_percentage_of_questions = funcs.get_est_percentage_of_questions(channel_df)
            channel_day.posters_pie_chart = funcs.get_posters_pie_chart(channel_df, channel_unique_users)
            
            yield channel_day.__dict__


for df in analyze_daily_data(each_day_dfs):  
    # TODO: Put it all in CSVs baby!
    channel_day_df = pd.DataFrame(data=d.__dict__, index=[1])
    channel_day_df.to_csv("info/test_format.csv", encoding="utf-8", index=False)



# d = channel_days[0]
# channel_day_df = pd.DataFrame(data=d.__dict__, index=[1])
# channel_day_df.to_csv("info/test_format.csv", encoding="utf-8", index=False)

# frames = []
# for cd in channel_days:
#     frames.append(pd.DataFrame(data=cd.__dict__, index=[1]))
