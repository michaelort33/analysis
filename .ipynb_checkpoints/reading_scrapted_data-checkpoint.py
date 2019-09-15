#%% imports
import re
import pandas as pd
import numpy as np

#%% read data
scraped_data = pd.read_csv('nj_scrape_cities_6_11.csv')

#%% clean data

# count which column has the most NaN
na_counts = scraped_data.apply(lambda x: len(x)-len(x.dropna()))

# get which has the lowest na
lowest_na = na_counts[na_counts == na_counts[na_counts>0].min()].index.values

# filter out all na without lowest_na

scraped_data = scraped_data.dropna(subset=lowest_na)

# filter for has historical chart
has_history = (scraped_data.loc['avg_daily_svg'].str.len() > 10 & scraped_data.loc['avg_occupancy_svg'].str.len() > 10 & scraped_data.loc['avg_revenue_svg'].str.len() >10)
scraped_data_has_history = scraped_data.loc[has_history]

#%%

def extract_numbers(string):
    return (re.findall(r'\d+', string))


def get_end_points(d_path):
    all_points = d_path.split(',')

    end_points = []
    for point in all_points:
        if 'C' in point:
            end = re.match("(.*?)C", point).group()
            end = end[:-1]
            end = float(end)
            end_points.append(end)
    # add last point
    end_points.append(float(all_points[-1]))

    return (end_points)


def convert_end_points_to_value(end_points, extrema):
    diff = extrema['max'] - extrema['min']
    end_point_value = -diff / 40
    end_point_start = extrema['max']
    end_points = [(end_point_value * i) + end_point_start for i in end_points]
    return (end_points)


max_monthly_revenue_last_12_months = ''
min_monthly_revenue_last_12_months = ''
max_occupancy_rate_last_12_months = ''
min_occupancy_rate_last_12_months = ''
max_nightly_revenue_last_12_months = ''
min_nightly_revenue_last_12_months = ''

avg_revenue_svg = 'd_path'
avg_occupancy_svg = 'd_path'
avg_daily_svg = 'd_path'
# get revenue for last 12
my_revenue_max = extract_numbers(max_monthly_revenue_last_12_months)
my_revenue_min = extract_numbers(min_monthly_revenue_last_12_months)
svg_points_last_12_months_revenue = get_end_points(avg_revenue_svg)
last_12_months_revenue_extrema = {'max': my_revenue_max,
                                  'min': my_revenue_min}
last_12_months_revenue = convert_end_points_to_value(svg_points_last_12_months_revenue,
                                                     last_12_months_revenue_extrema)

# get occupancy for last 12
my_occupancy_max = extract_numbers(max_occupancy_rate_last_12_months)
my_occupancy_min = extract_numbers(min_occupancy_rate_last_12_months)
svg_points_last_12_months_occupancy = get_end_points(avg_occupancy_svg)
last_12_months_occupancy_extrema = {'max': my_occupancy_max,
                                    'min': my_occupancy_min}
last_12_months_occupancy = convert_end_points_to_value(svg_points_last_12_months_occupancy,
                                                       last_12_months_occupancy_extrema)

# get average nightly rates for last 12
my_nightly_max = extract_numbers(max_nightly_revenue_last_12_months)
my_nightly_min = extract_numbers(min_nightly_revenue_last_12_months)
svg_points_last_12_months_nightly = get_end_points(avg_daily_svg)
last_12_months_nightly_extrema = {'max': my_nightly_max,
                                  'min': my_nightly_min}
last_12_months_nightly = convert_end_points_to_value(last_12_months_nightly_extrema,
                                                     svg_points_last_12_months_nightly)