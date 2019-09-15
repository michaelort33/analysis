import datetime
import re
import numpy as np
import pandas as pd
from tqdm import tqdm

# global variables
all_dates = [datetime.date(2019, 9, 1), datetime.date(2019, 8, 10), datetime.date(2019, 7, 26),
             datetime.date(2019, 7, 1)]

scrape_date = all_dates[0]
last_scrape_date = all_dates[1]

# don't have checker date yet this should be older
checker_date = all_dates[2]

# all dates for selecting best estimates
# read the new csv here
df = pd.read_csv('data/zip_9_01_1_5.csv')

# read current database here
old = pd.read_pickle('data/final_database_frames/db_2019_9_01_v1_p9_zip.pkl')


def isNaN(num):
    return num != num


def extract_numbers(series):
    numbers = []
    for string in series:
        if isNaN(string):
            joined_nums = string
        else:
            nums = re.findall(r'[K.\d]+', string)
            joined_nums = ''.join(nums)
            if 'K' in joined_nums:
                joined_nums = joined_nums.replace('K', '')
                joined_nums = joined_nums + '00'
                if '.' in joined_nums:
                    joined_nums = joined_nums.replace('.', '')
                else:
                    joined_nums = joined_nums + '0'

            if len(joined_nums) > 0:
                joined_nums = float(joined_nums)
            else:
                joined_nums = np.nan
        numbers.append(joined_nums)
    return numbers


def basic_cleaning(df):
    # drop pointless column
    df = df.drop(['_type'], axis=1)

    # change num of listings to float
    # first change commas to nothing
    df.current_active_listings = [
        i.replace(',', '') if type(i) is str else i
        for i in df.current_active_listings
    ]
    # then change to float
    df.current_active_listings = [
        float(i) if type(i) is str else i for i in df.current_active_listings
    ]

    # change active_listing_tics_2 to numbers from strings
    df.active_listing_tics_2 = extract_numbers(df.active_listing_tics_2)

    # change error message for duplicate error to a simple boolean yes
    df['duplicate_error'] = [
        1 if i == 'We\'re sorry, but there seems to be a problem.' else 0
        for i in df.duplicate_error
    ]

    return df


df = basic_cleaning(df)


# Use all points or svg with 1 as the newest frame and 2 as last month
# Get svg data
def get_end_points(d_path):
    all_points = d_path.split(',')

    end_points = []
    for point in all_points:
        if 'C' in point:
            end = re.match("(.*?)C", point).group()
            end = end[:-1]
            end = float(end)
            end_points.append(end)
    # add last point but only if there was a C in the dpath
    # will have to be dealt with later
    if len(end_points) > 0:
        last_point = all_points[-1]
        end_points.append(float(last_point))

    return end_points


def convert_end_points_to_value_zip(end_points, two_points,
                                    two_points_svg_value):
    # get difference in dollars between this month and last month
    diff = two_points['point_1'] - two_points['point_2']

    # get difference in pixels between position of this month and last month
    svg_point_diff = two_points_svg_value[
                         'point_1_svg'] - two_points_svg_value['point_2_svg']

    # If diff is 0 then give up on calculating
    if svg_point_diff == 0:
        end_points = np.nan
    else:
        # calculate dollars per pixel
        end_point_value = diff / svg_point_diff

        # pick either end point to calculate start value at endpoint = 0
        end_point_start = two_points['point_1'] - (
                two_points_svg_value['point_1_svg'] * end_point_value)
        end_points = [(end_point_value * i) + end_point_start
                      for i in end_points]

    return end_points


def get_svg_data_zip(point_1, point_2, d_path_1):
    # get revenue for last 12
    svg_points_last_12_months_new = get_end_points(d_path_1)

    if len(svg_points_last_12_months_new) > 1 and not isNaN(np.sum(svg_points_last_12_months_new)):
        point_1_svg = svg_points_last_12_months_new[-1]
        point_2_svg = svg_points_last_12_months_new[-2]

        last_12_months_two_points = {'point_1': point_1, 'point_2': point_2}
        last_12_months_two_points_svg_value = {
            'point_1_svg': point_1_svg,
            'point_2_svg': point_2_svg
        }

        last_12_months = convert_end_points_to_value_zip(
            svg_points_last_12_months_new, last_12_months_two_points,
            last_12_months_two_points_svg_value)
    else:
        last_12_months = np.nan
    return last_12_months


def get_svg_history_zip(all_point_1, all_point_2, svg_data_1):
    avg_last_12_months = []
    for i, j, k in zip(all_point_1, all_point_2, svg_data_1):
        point_1 = i
        point_2 = j
        d_path_1 = k

        if not isNaN(point_1) and not isNaN(point_2) and isinstance(
                d_path_1, str):
            historical_values = get_svg_data_zip(point_1, point_2, d_path_1)
        else:
            historical_values = np.nan

        avg_last_12_months.append(historical_values)

        # convert blank string to nan
    return avg_last_12_months


def get_meta_data(meta_data, my_eg_ex):
    # get last month revenue from zip_meta
    meta_data = meta_data.str.extract(my_eg_ex).iloc[:, 0]
    # replace commas
    meta_data = [
        i.replace(',', '') if type(i) is str else i for i in meta_data
    ]
    # change occupancy to floats
    meta_data = [float(i) if type(i) is str else i for i in meta_data]
    return meta_data


def add_extracted_columns(df):
    # add city state zip string from url
    df['state_city_zip'] = df.url.str.extract('data/app/us/(.*)/overview')

    # add city state
    df['state_city'] = df.url.str.extract(r'data/app/us/(.*)/\d+')

    # change rental_size to numbers and guests to numbers and convert to float
    df['rooms'] = [
        float(i) for i in df.rental_size.str.extract(pat='^(\S+)').values
    ]
    df['guests'] = [
        float(i) for i in df.rental_size.str.extract(
            pat='Bedrooms / (.*) Guests').values
    ]

    # get last month occupancy from zip_meta
    df['occupancy_rate_last_month_from_meta_' +
       str(scrape_date)] = get_meta_data(df.zip_meta_rates,
                                         'HomeAway average (.*)% occupancy')

    # get last month nightly from zip_meta
    df['nightly_revenue_last_month_from_meta_' +
       str(scrape_date)] = get_meta_data(df.zip_meta_rates,
                                         '\$(.*) daily rate')

    # get last month revenue from zip_meta
    df['monthly_revenue_last_month_from_meta_' +
       str(scrape_date)] = get_meta_data(df.zip_meta_rates,
                                         'and \$(.*) in revenue')
    return df


df = add_extracted_columns(df)


def add_old_data_for_svg(df, old):
    # read database data

    # add some database data to new_data for svg_translation
    old_svg_data = old[[
        'occupancy_rate_last_month_from_meta_' + str(last_scrape_date),
        'nightly_revenue_last_month_from_meta_' + str(last_scrape_date),
        'monthly_revenue_last_month_from_meta_' + str(last_scrape_date),
        'state_city_zip'
    ]]

    df = df.merge(old_svg_data, on='state_city_zip', how='left')
    return df


df = add_old_data_for_svg(df, old)


# use the get end points function to store end points as their own columns for validation
def get_end_points_value_validation(point_1, point_2, svg_data):
    end_points = []
    end_point_values = []
    for d_path, i, k in zip(svg_data, point_1, point_2):
        if not isNaN(i) and not isNaN(k) and isinstance(
                d_path, str):
            single_end_points = get_end_points(d_path)
            if len(single_end_points) > 1 and (single_end_points[-1] - single_end_points[-2]) != 0:
                single_end_point_value = (i - k) / (single_end_points[-1] - single_end_points[-2])
            else:
                single_end_point_value = np.nan
        else:
            single_end_points = np.nan
            single_end_point_value = np.nan
        end_points.append(single_end_points)
        end_point_values.append(single_end_point_value)

    return {'end_points': end_points, 'end_point_values': end_point_values}


def get_negative_validation(data_history):
    negatives = []
    for i in data_history:
        if type(i) is list:
            if len(i) > 0:
                negatives.append(any(k < 0 for k in i))
            else:
                negatives.append(False)
        else:
            negatives.append(False)

    return negatives


def add_svg_data(df):
    # first make new monthly revenue column for the new scrape alone
    df['monthly_revenue' + str(scrape_date)] = get_svg_history_zip(
        df['monthly_revenue_last_month_from_meta_' + str(scrape_date)],
        df['monthly_revenue_last_month_from_meta_' + str(last_scrape_date)],
        df.avg_revenue_svg)

    # now add a duplicate that will be merged with previous scrapes
    df['monthly_revenue'] = df['monthly_revenue' + str(scrape_date)]

    # add end points value validation
    end_point_monthly = get_end_points_value_validation(df['monthly_revenue_last_month_from_meta_' + str(scrape_date)],
                                                        df['monthly_revenue_last_month_from_meta_' + str(
                                                            last_scrape_date)],
                                                        df.avg_revenue_svg)

    df['end_points_monthly_revenue' + str(scrape_date)] = end_point_monthly['end_points']
    df['end_point_values_monthly_revenue' + str(scrape_date)] = end_point_monthly['end_point_values']

    # add negative revenue validation
    df['monthly_revenue_negative_validation' + str(scrape_date)] = get_negative_validation(
        df['monthly_revenue' + str(scrape_date)])

    # first make new nightly revenue column for the new scrape alone
    df['nightly_revenue' + str(scrape_date)] = get_svg_history_zip(
        df['nightly_revenue_last_month_from_meta_' + str(scrape_date)],
        df['nightly_revenue_last_month_from_meta_' + str(last_scrape_date)],
        df.avg_daily_svg)

    # now add a duplicate that will be merged with previous scrapes
    df['nightly_revenue'] = df['nightly_revenue' + str(scrape_date)]

    # add end points value validation
    end_point_nightly = get_end_points_value_validation(df['nightly_revenue_last_month_from_meta_' + str(scrape_date)],
                                                        df['nightly_revenue_last_month_from_meta_' + str(
                                                            last_scrape_date)],
                                                        df.avg_daily_svg)

    df['end_points_nightly_revenue' + str(scrape_date)] = end_point_nightly['end_points']
    df['end_point_values_nightly_revenue' + str(scrape_date)] = end_point_nightly['end_point_values']

    # add negative revenue validation
    df['nightly_revenue_negative_validation' + str(scrape_date)] = get_negative_validation(
        df['nightly_revenue' + str(scrape_date)])

    # first make new monthly occupancy column for the new scrape alone
    df['monthly_occupancy' + str(scrape_date)] = get_svg_history_zip(
        df['occupancy_rate_last_month_from_meta_' + str(scrape_date)],
        df['occupancy_rate_last_month_from_meta_' + str(last_scrape_date)],
        df.avg_occupancy_svg)

    # now add a duplicate that will be merged with previous scrapes
    df['monthly_occupancy'] = df['monthly_occupancy' + str(scrape_date)]

    # add end points value validation
    end_point_occupancy = get_end_points_value_validation(df['occupancy_rate_last_month_from_meta_' + str(scrape_date)],
                                                          df['occupancy_rate_last_month_from_meta_' + str(
                                                              last_scrape_date)],
                                                          df.avg_daily_svg)

    df['end_points_monthly_occupancy' + str(scrape_date)] = end_point_occupancy['end_points']
    df['end_point_values_monthly_occupancy' + str(scrape_date)] = end_point_occupancy['end_point_values']

    # add negative occupancy validation
    df['monthly_occupancy_negative_validation' + str(scrape_date)] = get_negative_validation(
        df['monthly_occupancy' + str(scrape_date)])

    return df


df = add_svg_data(df)


def add_score_validity(df):
    df['nightly_revenue_valid'] = df['nightly_revenue_negative_validation' + str(scrape_date)] | (
            df['end_point_values_nightly_revenue' + str(scrape_date)] <= 0)
    df['monthly_revenue_valid'] = df['monthly_revenue_negative_validation' + str(scrape_date)] | (
            df['end_point_values_monthly_revenue' + str(scrape_date)] <= 0)
    df['monthly_occupancy_valid'] = df['monthly_occupancy_negative_validation' + str(
        scrape_date)] | (df['end_point_values_monthly_occupancy' + str(scrape_date)] <= 0)

    return df


df = add_score_validity(df)


def add_new_columns(df, old):
    # get columns not currently on database (if any)
    new_cols = list(df.columns.difference(old.columns))
    # add the id column we will merge on.
    new_cols.append('state_city_zip')
    # merge the new cols
    my_updated_old = old.merge(df[new_cols], how='outer', on='state_city_zip')

    return my_updated_old


updated_old = add_new_columns(df, old)


def update_old_cols(df, updated_old, needs_updating, needs_validation):
    added_revenue_cols = needs_updating + ['state_city_zip']
    added_revenue = updated_old[added_revenue_cols].merge(df[added_revenue_cols],
                                                          how='outer',
                                                          on='state_city_zip')

    for col_name in tqdm(needs_updating):

        added_revenue = added_revenue.astype({col_name + '_x': 'object'})

        for idx, i in enumerate(added_revenue.iterrows()):
            if not isNaN(i[1][col_name + '_y']):
                if col_name in needs_validation:
                    if i[1][col_name + '_valid_y'] == False:
                        added_revenue.at[idx, col_name + '_x'] = i[1][col_name + '_y']
                else:
                    added_revenue.at[idx, col_name + '_x'] = i[1][col_name + '_y']

        updated_old[col_name] = added_revenue[col_name + '_x']

    return updated_old


def add_new_svg_set(df, updated_old):
    needs_updating = ['active_listing_tics_2', 'current_active_listings',
                      'entire_home', 'pct_available_full_time',
                      'percent_entire_listing', 'private_room',
                      'q1', 'q10', 'q11', 'q12', 'q13', 'q2', 'q3', 'q4', 'q5', 'q6',
                      'q7', 'q8', 'q9', 'quarterly_growth',
                      'range_quarterly_listings_history_end',
                      'range_quarterly_listings_history_start', 'rated_at_least_4_5',
                      'rating_avg', 'rental_size', 'shared_room',
                      'rooms', 'guests', 'nightly_revenue', 'monthly_revenue', 'monthly_occupancy',
                      'nightly_revenue' + str(scrape_date), 'monthly_revenue' + str(scrape_date),
                      'monthly_occupancy' + str(scrape_date),
                      'monthly_revenue_last_month_from_meta_' + str(scrape_date),
                      'nightly_revenue_last_month_from_meta_' + str(scrape_date),
                      'occupancy_rate_last_month_from_meta_' + str(scrape_date),
                      'end_points_monthly_revenue' + str(scrape_date),
                      'end_point_values_monthly_revenue' + str(scrape_date),
                      'avg_revenue_svg', 'avg_occupancy_svg', 'avg_daily_svg',
                      'nightly_revenue_valid', 'monthly_revenue_valid', 'monthly_occupancy_valid']

    needs_validation = ['nightly_revenue', 'monthly_revenue', 'monthly_occupancy']

    updated_old = update_old_cols(df, updated_old, needs_updating, needs_validation)

    return updated_old


updated_old = add_new_svg_set(df, updated_old)


def get_svg_avg(my_history):
    avg = []
    for i in my_history:
        if type(i) is list:
            if len(i) > 0:
                avg.append(np.average(i))

            else:
                avg.append(np.nan)
        else:
            avg.append(np.nan)

    return avg


def get_svg_cv(my_history):
    cv = []

    for i in my_history:
        if type(i) is list:
            if len(i) > 0:
                cv.append(np.std(i) / np.average(i))
            else:
                cv.append(np.nan)
        else:
            cv.append(np.nan)
    return cv


def get_max_svg(my_history):
    my_max = []

    for i in my_history:
        if type(i) is list:
            if len(i) > 0:
                my_max.append(max(i))
            else:
                my_max.append(np.nan)
        else:
            my_max.append(np.nan)
    return my_max


def get_min_svg(my_history):
    my_min = []

    for i in my_history:
        if type(i) is list:
            if len(i) > 0:
                my_min.append(min(i))
            else:
                my_min.append(np.nan)
        else:
            my_min.append(np.nan)
    return my_min


def get_last_svg(my_history):
    my_last = []

    for i in my_history:
        if type(i) is list:
            if len(i) > 0:
                my_last.append(i[-1])
            else:
                my_last.append(np.nan)
        else:
            my_last.append(np.nan)
    return my_last


def add_extrapolated_revenue(updated_old):
    # now that info is finalized add the extrapolated revenue data

    # average of lists of history of svg
    updated_old['avg_monthly_revenue'] = get_svg_avg(
        updated_old['monthly_revenue'])

    updated_old['avg_nightly_revenue'] = get_svg_avg(
        updated_old['nightly_revenue'])

    updated_old['avg_monthly_occupancy'] = get_svg_avg(
        updated_old['monthly_occupancy'])

    # seasonality measure
    updated_old['seasonality_monthly_revenue'] = get_svg_cv(
        updated_old['monthly_revenue'])

    updated_old['seasonality_nightly_revenue'] = get_svg_cv(
        updated_old['nightly_revenue'])

    updated_old['seasonality_monthly_occupancy'] = get_svg_cv(
        updated_old['monthly_occupancy'])

    # divide revenue by rooms and guests to get average per room and per guest
    updated_old['revenue_per_room'] = updated_old[
        'avg_monthly_revenue'].divide(updated_old['rooms'])
    updated_old['revenue_per_guest'] = updated_old[
        'avg_monthly_revenue'].divide(updated_old['guests'])

    # add revenue ratio
    updated_old['rent_to_rent_ratio'] = updated_old['revenue_per_room'] / (
            1000 * updated_old['2019-06'])

    # max min and last
    updated_old['max_monthly_revenue_last_12_months_extracted'] = get_max_svg(updated_old['monthly_revenue'])
    updated_old['max_nightly_revenue_last_12_months_extracted'] = get_max_svg(updated_old['nightly_revenue'])
    updated_old['max_occupancy_rate_last_12_months_extracted'] = get_max_svg(updated_old['monthly_occupancy'])

    updated_old['min_monthly_revenue_last_12_months_extracted'] = get_min_svg(updated_old['monthly_revenue'])
    updated_old['min_nightly_revenue_last_12_months_extracted'] = get_min_svg(updated_old['nightly_revenue'])
    updated_old['min_occupancy_rate_last_12_months_extracted'] = get_min_svg(updated_old['monthly_occupancy'])

    updated_old['monthly_revenue_last_month_extracted'] = get_last_svg(updated_old['monthly_revenue'])
    updated_old['nightly_revenue_last_month_extracted'] = get_last_svg(updated_old['nightly_revenue'])
    updated_old['occupancy_rate_last_month_extracted'] = get_last_svg(updated_old['monthly_occupancy'])

    return updated_old


updated_old = add_extrapolated_revenue(updated_old)


# this updates the database
# updated_old.to_pickle('data/final_database_frames/db_2019_9_01_v1_zip.pkl')

# only use after having three months
# checks if predicted three months-ago revenue is more than 1% off of
# the actual meta scrape from three months ago
# return list of dictionaries for the two compared values and their index row number in updated_old

def checker(updated_old):
    my_idx = []
    for idx, i in enumerate(updated_old.iterrows()):
        if not isNaN(i[1]['monthly_revenue']):
            if len(i[1]['monthly_revenue']) > 2 and not isNaN(
                    i[1]['monthly_revenue_last_month_from_meta_' + str(checker_date)]):
                num_1 = round(i[1]['monthly_revenue'][-3])
                num_2 = i[1]['monthly_revenue_last_month_from_meta_' + str(checker_date)]
                if (abs(num_1 - num_2)) > (0.01 * num_2):
                    my_idx.append(
                        {'index': idx, 'monthly': i[1]['monthly_revenue'][-3],
                         'checker': i[1]['monthly_revenue_last_month_from_meta_2019-07-26']})

    return my_idx
