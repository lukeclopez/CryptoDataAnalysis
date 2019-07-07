import pandas as pd
import pathlib as pl
import code
import json
import os


def create_df_for_each_relevant_table(relevant_tables_path, engine, limit=None):
    """
    1. Read a file containing the name of a message table on each line.
    2. Turn each table into a dataframe.
    3. Add each new dataframe to the list.
    4. Return the list.
    """
    dataframes = []
    counter = 0

    csv_folder = pl.Path.cwd() / "info" / "csv"

    with open(relevant_tables_path) as relevant_tables:
        for table_name in relevant_tables:
            table_name = table_name.strip()
            print("Reading table:", table_name)
            df = pd.read_sql_table(table_name, engine)
            # Save df to CSV
            df.to_csv(csv_folder / f"{table_name}.csv", encoding="utf-8", index=False)
            dataframes.append(df)
            counter += 1
            if limit and counter > limit:
                break


    return dataframes


def get_unique_channels(df):
        channel_ids = df["channel_id"].values

        return set(channel_ids)


def get_channel_dfs_list(df, channel_id_list):
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


def get_posters_pie_chart(channel_df, user_ids_unique, round_digits=4):
    posters_pie_chart = {}

    for uuid in user_ids_unique:
        user_rows = channel_df.loc[channel_df["user_id"] == uuid]

        posters_pie_chart[int(uuid)] = round(len(user_rows) / len(channel_df), round_digits)

    return json.dumps(posters_pie_chart)


def get_channel_id(channel_df):
    return channel_df.iloc[0]["channel_id"]