import pandas as pd
import code
import json
import pathlib as pl
import sqlalchemy
import utils
import os
import dict_models
from pprint import pprint

HOST = "ca-pro-rds-prod.cllw6gj7ocf7.us-east-1.rds.amazonaws.com"
DB_NAME = "telegram_data"
DB_USER =  "analytics"
DB_PASS = "acaa301cf8214359b9bb9722b3aa469f"

engine = sqlalchemy.create_engine(f"postgres://{DB_USER}:{DB_PASS}@{HOST}:5432/{DB_NAME}")

channel_names = pd.read_sql_table("message_service_channel", engine)
channel_names.to_csv("channel_names.csv", encoding="utf-8", index=False)

# Filepaths
relevant_tables = pl.Path("info/relevant_tables.txt")  # Tables to include in analysis
csv_folder = pl.Path("info/message_table_csvs")  # Where to save csvs of info read from DB
output = pl.Path("info/output/")  # Where to save processed data

channel_day = dict_models.channel_day


def analyze_each_channel_in_day(day_of_data_df):
    """
    1. Get list of dataframes, one for each channel.
    2. For each dataframe, update the values of the channel_day dict.
    3. Yield the channel day dict as a PANDAS dataframe

    This function is a generator because each day of data can have many channels.
    Returning ends the execution, while yielding lets the loop continue.
    """
    channel_dfs = utils.get_channel_dfs_list(day_of_data_df)

    for channel_df in channel_dfs:
        channel_day["channel_id"] = utils.get_channel_id(channel_df)
        channel_day["channel_username"] = utils.get_channel_username(channel_day["channel_id"])
        channel_day["channel_title"] = utils.get_channel_title(channel_day["channel_id"])
        channel_day["date"] = utils.get_date(channel_df)
        channel_day["unique_posters"] = utils.get_unique_posters_count(channel_df)
        channel_day["total_messages"] = utils.get_message_count(channel_df)
        channel_day["est_percentage_of_questions"] = utils.get_est_percentage_of_questions(channel_df)
        channel_day["posters_pie_chart"] = utils.get_posters_pie_chart(channel_df)

        yield pd.DataFrame(data=channel_day, index=[1])


def get_processed_daily_(dfs, save_dfs=False):
    """
    This function takes an iterable of dfs.
    If working with dataframes directly, use 'create_df_for_each_relevant_table(relevant_tables, engine)'
    If working with CSVs, use 'get_dfs_from_csvs(csv_folder)'.
    Both of these functions are in utils.py
    """
    for raw_day_df in dfs:
        rows = [i for i in analyze_each_channel_in_day(raw_day_df)]

        finished_day_of_data_df = pd.concat(rows)

        if save_dfs:
            string_date = utils.get_date(raw_day_df, string=True)
            finished_day_of_data_df.to_csv(output / f"{string_date}.csv", encoding="utf-8", index=False)

        yield finished_day_of_data_df


if __name__ == "__main__":
    raw_day_dfs = utils.create_df_for_each_relevant_table(relevant_tables, engine, limit=3)
    output = list(get_processed_daily_(raw_day_dfs, save_dfs=True))
     
    code.interact(local=locals())
