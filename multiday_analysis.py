import pandas as pd
import code
import json
import pathlib as pl
import sqlalchemy
import statistics
import utils
import os
from pprint import pprint
from dict_models import channel_period


channel_data_csvs_folder_path = pl.Path("info/output")

# Make a giant df with all days and all channels in the given folder
all_day_dfs = [i for i in utils.get_dfs_from_csvs(channel_data_csvs_folder_path)]
giant_df = pd.concat(all_day_dfs)

channel_ids = utils.get_unique_channels(giant_df)
channel_dfs = utils.get_channel_dfs_list(giant_df, channel_ids)

# Organize the giant df by channels
for channel_df in channel_dfs:
    file_name = utils.get_channel_id(channel_df)
    channel_df.to_csv(f"info/final output/{file_name}.csv", encoding="utf-8", index=False)

# Use chunks to split up dfs by 7 and 30-day periods.
channel_data_csvs_folder_path = pl.Path("info/final output")

period_length_in_days = 7

channel_data_dfs = [i for i in utils.get_dfs_from_csvs(channel_data_csvs_folder_path, chunksize=period_length_in_days)]

channel_period_dfs_list = []

channel_period = dict_models.channel_period

for chunk in channel_data_dfs:
    # Find average daily posters
    try:
        unique_posters_values = chunk["unique_posters"].values
    except TypeError:
        continue

    avg_daily_posters = statistics.mean(unique_posters_values)

    # Number of total messages
    total_messages_values = chunk["total_messages"].values
    total_messages_period = sum(total_messages_values)

    # Estimated percentage of questions
    est_percentage_of_questions_values = chunk["est_percentage_of_questions"].values
    est_percentage_of_questions_period = statistics.mean(est_percentage_of_questions_values)

    pies = chunk["posters_pie_chart"].values

    pie_charts_list = []

    for pie in pies:
        pie = json.loads(pie)
        pie_charts_list.append(pie)

    new_chart = utils.get_cumulative_percentage(*pie_charts_list)

    # Find number of unique posters for this chunk
    unique_posters_period = len(new_chart)

    channel_period["channel_id"] = chunk["channel_id"].iloc[0].value
    channel_period["period_length_in_days"] = period_length_in_days
    channel_period["unique_posters"] = unique_posters_period
    channel_period["avg_daily_posters"] = avg_daily_posters
    channel_period["total_messages"] = total_messages_period
    channel_period["est_percentage_of_questions"] = est_percentage_of_questions_period
    channel_period["posters_pie_chart"] = json.dumps(new_chart)

    channel_period_df = pd.DataFrame(channel_period, index=[1])
    channel_period_dfs_list.append(channel_period_df)


final = pd.concat(channel_period_dfs_list)
final.to_csv(f"info/final/test.csv", encoding="utf-8", index=False)

# giant_df = pd.concat(all_day_dfs)

# channel_ids = utils.get_unique_channels(giant_df)
# channel_dfs = utils.get_channel_dfs_list(giant_df, channel_ids)


code.interact(local=locals())
