from datetime import datetime
import pandas as pd
import pathlib as pl
import code
import json
import re
import os
from pprint import pprint


message_table_csvs = pl.Path("info/message_table_csvs")

def create_df_for_each_relevant_table(relevant_tables_path, engine, limit=None, save_dfs=False):
    """
    1. Read a file containing the name of a message table on each line.
    2. Turn each table into a dataframe.
    3. Add each new dataframe to the list.
    4. Return the list of dataframes.
    """
    dataframes = []
    counter = 0

    with open(relevant_tables_path) as relevant_tables:
        for table_name in relevant_tables:
            table_name = table_name.strip()

            df = pd.read_sql_table(table_name, engine)

            # There is an option to save the dfs as CSVs so you don't have to repeatedly process them if testing.
            if save_dfs:
                df.to_csv(message_table_csvs / f"{table_name}.csv", encoding="utf-8", index=False)

            dataframes.append(df)

            # I included the below code so I could limit 
            # the amount of tables processed if I was only testing the code.
            counter += 1
            if limit and counter > limit:
                break

    return dataframes


def get_unique_channels(df):
    channel_ids = df["channel_id"].values

    return set(channel_ids)


def get_channel_dfs_list(df):
    channel_id_list = get_unique_channels(df)
    channel_dfs_list = []

    for channel_id in channel_id_list:
        channel_df = df.loc[df["channel_id"] == channel_id]
        channel_dfs_list.append(channel_df)

    return channel_dfs_list


def get_unique_posters_count(channel_df):
    user_ids = channel_df["user_id"].values
    user_ids_unique = set(user_ids)
    
    return len(user_ids_unique)


def get_unique_posters_list(channel_df):
    user_ids = channel_df["user_id"].values
    user_ids_unique = set(user_ids)
    
    return user_ids_unique
 

def get_message_count(channel_df):
    return len(channel_df)


def get_est_percentage_of_questions(channel_df, round_digits=4):
    # Find estimated percentage of questions
    question_rows = channel_df.loc[channel_df["message_text"].str.contains("\?")]
    no_of_questions = len(question_rows)

    return round(no_of_questions / len(channel_df), round_digits)


def get_posters_pie_chart(channel_df, round_digits=4):
    user_ids_unique= get_unique_posters_list(channel_df)
    
    posters_pie_chart = {}

    for uuid in user_ids_unique:
        user_rows = channel_df.loc[channel_df["user_id"] == uuid]

        posters_pie_chart[int(uuid)] = round(len(user_rows) / len(channel_df), round_digits)

    return json.dumps(posters_pie_chart)


def get_channel_username(channel_id):
    path = pl.Path("info/channel_names.csv")
    channel_names_df = pd.read_csv(path)
    channel_row = channel_names_df.loc[channel_names_df["id"] == channel_id]

    return channel_row["channel_username"].iloc[0]


def get_channel_title(channel_id):
    path = pl.Path("info/channel_names.csv")
    channel_names_df = pd.read_csv(path)
    channel_row = channel_names_df.loc[channel_names_df["id"] == channel_id]

    return channel_row["channel_title"].iloc[0]


def get_channel_id(channel_df):
    return channel_df.iloc[0]["channel_id"]


def get_date(channel_df, string=False):
    date_string_full = str(channel_df.iloc[0]["created_at"])
    date_string = re.findall(r"\d+-\d+-\d+", date_string_full)[0]

    if string:
        return date_string

    date = datetime.strptime(date_string, "%Y-%m-%d")
    
    return date


def get_dfs_from_csvs(csv_folder_path, chunksize=None):
    each_day_csvs = [f for f in os.listdir(csv_folder_path)]

    for csv_file in each_day_csvs:
        if chunksize:
            new_df = pd.read_csv(csv_folder_path / csv_file, chunksize=chunksize)
            for chunk in new_df:
                yield chunk
        else:
            new_df = pd.read_csv(csv_folder_path / csv_file)

        yield new_df


def get_cumulative_percentage(*args, round_digits=4): 
    # Find all unique keys
    keys = []
    for d in args:
        keys.extend(list(d.keys()))

    unique_keys = set(keys)

    # Make a dictionary with all the unique keys as keys
    # and 0 for every value
    posters_and_percents = {v:0 for (k,v) in enumerate(unique_keys)}

    # Get the total value for every key.
    for key in unique_keys:
        for d in args:
            for dkey, value in d.items():
                if dkey == key:
                    posters_and_percents[key] += value

    # Find the average based on how many dictionaries were passed
    posters_and_percents = {k:round(v/len(args), round_digits) for (k,v) in posters_and_percents.items()}

    return posters_and_percents
