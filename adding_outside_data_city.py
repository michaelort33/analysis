import pandas as pd

# read database data
old = pd.read_pickle('data/final_database_frames/db_2019_8_12_city.pkl')

# read and prep location data
location_data = pd.read_csv('data/geographic_data/zip_code_database_cleaned.csv', converters={'zip': lambda x: str(x)})


def prep_location_data_for_city(df):
    # read data
    df_subset = df.loc[:, ['state_city', 'latitude', 'longitude', 'state']]

    # remove duplciate longitude for zip code to get it on a city level
    df_subset = df_subset.drop_duplicates(subset='state_city')

    return df_subset


location_data = prep_location_data_for_city(location_data)

# add location to scraped_data
updated_old = old.merge(location_data, on='state_city')

# read zillow data
zillow = pd.read_csv('data/zillow/prepared_zillow_data/City_ZriPerSqft_AllHomes_2019-07-31.csv')

# add zillow data
updated_old = updated_old.merge(zillow, on='state_city', how='left')

# updated_old.to_pickle('data/final_database_frames/db_2019_8_13_city.pkl')

