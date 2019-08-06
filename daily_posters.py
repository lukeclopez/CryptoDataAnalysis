from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import code

df = pd.read_csv('7day_periods.csv')

# Make two lists: One for X with channel names, one for y with unique posters
date_range = "2018-11-07 - 2018-11-13"

channel_titles = list(df.loc[df["period_date_range"] == date_range, "channel_title"])[:10]

unique_posters = list(df.loc[df["period_date_range"] == date_range, "avg_daily_posters"])[:10]

# Add the title
plt.title(f'Average Daily Posters by Channel | Date Range: {date_range}')

plt.xlabel("Channels")
plt.ylabel("Average Number of Daily Posters")

# Add the data
plt.bar(channel_titles, unique_posters, label="Channels and Posters")

# Add the legend (looks for the labels)
plt.legend()

# Add the grid
plt.grid(True)

# Padding on smaller screens
plt.tight_layout()

# Save Graph
# plt.savefig('plot.png')

# Make sure the xtick labels are legible
plt.xticks(channel_titles, channel_titles, rotation=-45, ha='left')

plt.show()