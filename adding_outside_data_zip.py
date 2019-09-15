import pandas as pd

# read database data
old = pd.read_pickle('data/final_database_frames/zip_a_v1.pkl')


# merge scraped data with location data to get longitudes
# read and prep location data
location_data = pd.read_csv('data/geographic_data/zip_code_database_cleaned.csv',
                            converters={'zip': lambda x: str(x)})


def prep_location_data_for_zip(df):
    # read data
    df_subset = df.loc[:, ['state_city_zip', 'latitude', 'longitude', 'state','zip']]

    return df_subset


location_data = prep_location_data_for_zip(location_data)

updated_old = old.merge(location_data, on='state_city_zip')

# read zillow data
zillow = pd.read_csv('data/zillow/prepped_zillow_data/Zip_ZriPerSqft_AllHomes2019-08-08-zip.csv')



