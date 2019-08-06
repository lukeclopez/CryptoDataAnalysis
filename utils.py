from datetime import datetime
import pandas as pd
import pathlib as pl
import code
import json
import re
import os
import settings
from pprint import pprint

log = settings.configure_logger("default", "utils.txt")

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
                log.info(f"File saved for {table_name}")

            dataframes.append(df)

            # I included the below code so I could limit 
            # the amount of tables processed if I was only testing the code.
            counter += 1
            if limit and counter > limit:
                break

    return dataframes


def get_unique_channels(df):
    """
    This function returns a set of the channel IDs to ensure
    that each channel ID only appears once.
    """
    channel_ids = df["channel_id"].values

    return set(channel_ids)


def get_channel_dfs_list(df):
    """
    This function splits the main dataframe up into smaller dataframes,
    one dataframe for each channel.
    """
    channel_id_list = get_unique_channels(df)
    channel_dfs_list = []

    for channel_id in channel_id_list:
        channel_df = df.loc[df["channel_id"] == channel_id]
        channel_dfs_list.append(channel_df)

    return channel_dfs_list


def get_unique_posters_count(channel_df):
    """
    After converting a list of user IDs to a set to remove duplicates,
    return the length of that set.

    `channel_df` would normally be a pandas dataframe dedicated to one specific channel.
    See `get_channel_dfs_list`,
    """
    user_ids = channel_df["user_id"].values
    user_ids_unique = set(user_ids)
    
    return len(user_ids_unique)


def get_unique_posters_list(channel_df):
    """
    Convert a list of user IDs to a set to remove duplicates.
    """
    user_ids = channel_df["user_id"].values
    user_ids_unique = set(user_ids)
    
    return user_ids_unique
 

def get_message_count(channel_df):
    return len(channel_df)


def get_est_percentage_of_questions(channel_df, round_digits=4):
    """
    1. Make a new dataframe with all the rows where there is a `?` in the message text.
    2. Find the length of that new dataframe.
    3. Divide the length of the new dataframe by the length of the channel dataframe.
    4. Round to the number specified in `round_digits` and return.
    """
    question_rows = channel_df.loc[channel_df["message_text"].str.contains("\?", na=False)]
    no_of_questions = len(question_rows)

    return round(no_of_questions / len(channel_df), round_digits)


def get_posters_pie_chart(channel_df, round_digits=4):
    """
    1. Make a separate dataframe for every unique user.
    2. Divide the length of each user's dataframe by the length of the channel dataframe,
       rounded to the to the number specified in `round_digits`.
    3. Add the result to the `posters_pie_chart` dictionary.
    4. When done with all users, turn this dictionary into JSON and return it.
    """
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

    try:
        return channel_row.iloc[0]["channel_username"]
    except IndexError:
        return "(No Username)"


def get_channel_title(channel_id):
    path = pl.Path("info/channel_names.csv")
    channel_names_df = pd.read_csv(path)
    channel_row = channel_names_df.loc[channel_names_df["id"] == channel_id]

    try:
        return channel_row.iloc[0]["channel_title"]
    except IndexError:
        return "(No Title)"


def get_channel_id(channel_df):
    return channel_df.iloc[0]["channel_id"]


def get_date(channel_df, string=False):
    date_string_full = str(channel_df.iloc[0]["created_at"])
    date_string = re.findall(r"\d+-\d+-\d+", date_string_full)[0]

    if string:
        return date_string

    date = datetime.strptime(date_string, "%Y-%m-%d")
    
    return date


def get_dfs_from_csvs(csv_folder_path, chunksize=None, start_file=None):
    """
    Note: The `start_file` parameter is used when some of the CSVs have already
    been turned into dataframes and you don't want to repeat the work. My environment
    (Python 3.7.3, Windows 10) seems to assume alphabetical order within folders.

    1. Make a list of all CSV files in the folder specified in `csv_folder_path`.
    2. Turn every CSV on the list into a dataframe, then yield it (if a starting
       file was specified, this process will not begin until it is found).
    """
    each_day_csvs = [f for f in os.listdir(csv_folder_path) if ".csv" in f]

    start_file_found = False

    for csv_file in each_day_csvs:
        if start_file == csv_file:
            log.info(f"Start file found")
            start_file_found = True

        if start_file and not start_file_found:
            continue

        if chunksize:
            new_df = pd.read_csv(csv_folder_path / csv_file, chunksize=chunksize)
            log.info(f"Chunked df was read.")
            for chunk in new_df:
                yield chunk
        else:
            log.info(f"Df was read.")
            new_df = pd.read_csv(csv_folder_path / csv_file)

        yield new_df


def get_cumulative_percentage(*args, round_digits=4): 
    """
    This function takes multiple dictionaries returned from the function `get_posters_pie_chart`
    and merges them into one big pie chart.

    1. Get a list of all unique keys (which are user IDs) in the given dictionaries.
    2. Make a big dictionary called `posters_and_percents` with all the user IDs as keys and 0 for every value.
    3. Loop through every user and every dictionary. When we find a percentage that belongs to him,
       add it to his key in the `posters_and_percents` dictionary.
    4. Divide every value in `posters_and_percents` by the total number of dictionaries given to the function.
    5. Put the results of that division (the averages) in a new dictionary and return it.
    """
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
