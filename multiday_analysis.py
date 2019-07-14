import pandas as pd
import code
import json
import pathlib as pl
import sqlalchemy
import statistics
import utils
import os
from pprint import pprint
import dict_models
import settings

log = settings.configure_logger("default", pl.Path("info/output") / "logs" / "multiday_analysis.txt")
channel_period = dict_models.channel_period

dailies_path = settings.dailies_path
final_path = settings.final_path
merges_path = settings.merges_path

def merge_days(save_dfs=False):
    # Make a giant df with all days and all channels in the given folder
    all_day_dfs = [i for i in utils.get_dfs_from_csvs(dailies_path)]
    log.info("Giant df list created")

    giant_df = pd.concat(all_day_dfs)
    log.info("Giant df created")

    # Organize the giant df by channels
    channel_ids = utils.get_unique_channels(giant_df)
    channel_dfs = utils.get_channel_dfs_list(giant_df)

    for channel_df in channel_dfs:
        if save_dfs:
            channel_id = utils.get_channel_id(channel_df)
            channel_title = utils.get_channel_title(channel_df)
            channel_df.to_csv(merges_path / f"{channel_title}-{channel_id}.csv", encoding="utf-8", index=False)

        yield channel_df


merged_days = list(merge_days(save_dfs=True))
log.info("DF days merged")

period_length_in_days = 7
channel_data_dfs = [i for i in utils.get_dfs_from_csvs(merges_path, chunksize=period_length_in_days)]
channel_period_dfs_list = []

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

    # Find date range
    start = chunk.iloc[0]["date"]
    end = chunk.iloc[-1]["date"]
    date_range = f"{start} - {end}"

    # Find number of unique posters for this chunk
    unique_posters_period = len(new_chart)

    channel_period["channel_id"] = chunk["channel_id"].iloc[0]
    channel_period["channel_username"] = chunk["channel_username"].iloc[0]
    channel_period["channel_title"] = chunk["channel_title"].iloc[0]
    channel_period["period_length_in_days"] = period_length_in_days
    channel_period["period_date_range"] = date_range
    channel_period["unique_posters"] = unique_posters_period
    channel_period["avg_daily_posters"] = avg_daily_posters
    channel_period["total_messages"] = total_messages_period
    channel_period["est_percentage_of_questions"] = est_percentage_of_questions_period
    channel_period["posters_pie_chart"] = json.dumps(new_chart)

    channel_period_df = pd.DataFrame(channel_period, index=[1])
    channel_period_dfs_list.append(channel_period_df)

    log.info(f"Chunk for period {date_range} was analyzed.")


final = pd.concat(channel_period_dfs_list)
final.to_csv(final_path / f"{period_length_in_days}day_periods.csv", encoding="utf-8", index=False)

code.interact(local=locals())