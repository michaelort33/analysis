import re
from tqdm import tqdm
import numpy as np
import pandas as pd

# read the new csv here
df = pd.read_csv('data/all_items_ne_7_27.csv')


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

    # change extrema to numbers
    df.loc[:, [
                  'max_monthly_revenue_last_12_months',
                  'max_nightly_revenue_last_12_months',
                  'max_occupancy_rate_last_12_months',
                  'min_monthly_revenue_last_12_months',
                  'min_nightly_revenue_last_12_months',
                  'min_occupancy_rate_last_12_months'
              ]] = df.loc[:, [
                                 'max_monthly_revenue_last_12_months',
                                 'max_nightly_revenue_last_12_months',
                                 'max_occupancy_rate_last_12_months',
                                 'min_monthly_revenue_last_12_months',
                                 'min_nightly_revenue_last_12_months',
                                 'min_occupancy_rate_last_12_months'
                             ]].apply(extract_numbers)

    # change error message for duplicate error to a simple boolean yes
    df['duplicate_error'] = [
        1 if i == 'We\'re sorry, but there seems to be a problem.' else 0
        for i in df.duplicate_error
    ]

    return df


df = basic_cleaning(df)


def add_extracted_columns(df):
    # add city state
    df['state_city'] = df.url.str.extract('data/app/us/(.*)/overview')

    # change rental_size to numbers and guests to numbers and convert to float
    df['rooms'] = [
        float(i) for i in df.rental_size.str.extract(pat='^(\S+)').values
    ]
    df['guests'] = [
        float(i) for i in df.rental_size.str.extract(
            pat='Bedrooms / (.*) Guests').values
    ]

    return df


df = add_extracted_columns(df)


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
    # add last point but only if there was a C in the dpath. Other types of dpaths like those with a Z
    # will have to be dealt with later
    if len(end_points) > 0:
        last_point = all_points[-1]
        end_points.append(float(last_point))

    return end_points


def convert_end_points_to_value(end_points, two_points):
    diff = two_points['point_1'] - two_points['point_2']
    end_point_value = -diff / 40
    end_point_start = two_points['point_1']
    end_points = [(end_point_value * i) + end_point_start for i in end_points]

    return end_points


def get_svg_data(point_1, point_2, d_path):
    # get revenue for last 12
    svg_points_last_12_months = get_end_points(d_path)
    last_12_months_two_points = {'point_1': point_1, 'point_2': point_2}
    last_12_months = convert_end_points_to_value(svg_points_last_12_months,
                                                 last_12_months_two_points)
    if len(last_12_months) == 0:
        last_12_months = np.nan

    return last_12_months


def get_svg_history(all_point_1, all_point_2, svg_data):
    avg_last_12_months = []
    for i, j, k in zip(all_point_1, all_point_2, svg_data):
        point_1 = i
        point_2 = j
        d_path = k

        if not isNaN(point_1) and not isNaN(point_2) and isinstance(d_path, str):
            historical_values = get_svg_data(point_1, point_2, d_path)
        else:
            historical_values = np.nan

        avg_last_12_months.append(historical_values)

        # convert blank string to nan
    return avg_last_12_months


def add_svg_data(df):
    # extract and add svg columns
    df['monthly_revenue'] = get_svg_history(df.max_monthly_revenue_last_12_months,
                                            df.min_monthly_revenue_last_12_months, df.avg_revenue_svg)
    df['nightly_revenue'] = get_svg_history(df.max_nightly_revenue_last_12_months,
                                            df.min_nightly_revenue_last_12_months, df.avg_daily_svg)
    df['monthly_occupancy'] = get_svg_history(df.max_occupancy_rate_last_12_months,
                                              df.min_occupancy_rate_last_12_months, df.avg_occupancy_svg)

    return df


df = add_svg_data(df)


# add new scraped columns to database
def add_new_columns(df, old):
    # get columns not currently on database (if any)
    new_cols = list(df.columns.difference(old.columns))
    # add the id column we will merge on.
    new_cols.append('state_city')
    # merge the new cols
    my_updated_old = old.merge(df[new_cols], how='outer', on='state_city')

    return my_updated_old


old = pd.read_pickle('data/final_database_frames/db_2019_8_15_city.pkl')

updated_old = add_new_columns(df, old)


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


def update_old_cols(df, updated_old, col_name):
    added_revenue = updated_old[[col_name, 'state_city'
                                 ]].merge(df[[col_name, 'state_city']],
                                          how='outer',
                                          on='state_city')

    for idx, i in enumerate(added_revenue.iterrows()):
        if not isNaN(i[1][col_name + '_y']):
            added_revenue.at[idx, col_name + '_x'] = i[1][col_name + '_y']

    updated_column = added_revenue[col_name + '_x']
    return updated_column


def add_new_svg_set(df, updated_old):
    needs_updating = ['active_listing_tics_2', 'current_active_listings',
                      'entire_home', 'pct_available_full_time',
                      'percent_entire_listing', 'private_room',
                      'q1', 'q10', 'q11', 'q12', 'q13', 'q2', 'q3', 'q4', 'q5', 'q6',
                      'q7', 'q8', 'q9', 'quarterly_growth',
                      'range_quarterly_listings_history_end',
                      'range_quarterly_listings_history_start', 'rated_at_least_4_5',
                      'rating_avg', 'rental_size', 'shared_room',
                      'rooms', 'guests', 'nightly_revenue', 'monthly_revenue', 'monthly_occupancy']

    for i in tqdm(needs_updating):
        updated_old[i] = update_old_cols(df, updated_old, i)

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


updated_old = add_new_svg_set(df, updated_old)

