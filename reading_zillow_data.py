import datetime
import json

import pandas as pd

# Global variables
# these are the end of url names to download economic data from zillow
# here: https://www.zillow.com/research/data/
names = "MedianRentalPricePerSqft_3Bedroom,MedianRentalPricePerSqft_2Bedroom,MedianRentalPricePerSqft_1Bedroom,MedianRentalPricePerSqft_Studio,MedianRentalPricePerSqft_Sfr,MedianRentalPricePerSqft_DuplexTriplex,MedianRentalPricePerSqft_CondoCoop,MedianRentalPricePerSqft_Mfr5Plus,MedianRentalPricePerSqft_AllHomes,MedianRentalPrice_5BedroomOrMore,MedianRentalPrice_4Bedroom,MedianRentalPrice_3Bedroom,MedianRentalPrice_2Bedroom,MedianRentalPrice_1Bedroom,MedianRentalPrice_Studio,MedianRentalPrice_Sfr,MedianRentalPrice_DuplexTriplex,MedianRentalPrice_CondoCoop,MedianRentalPrice_Mfr5Plus,MedianRentalPrice_AllHomes,ZriPerSqft_AllHomes,Zri_AllHomesPlusMultifamily_Summary"
names = names.split(',')
current_date = str(datetime.date(2019, 8, 8))
last_column_date = '2019-06'
prefix = 'Zip_'

# get region names from State abbreviations
def translate_state_to_region(states):
    with open('data/translators/states_region.json') as f:
        state_region = json.load(f)

    return [state_region[x] for x in states]


# get state names from abbreviations
def translate_state_names(states):
    with open('data/translators/states_translate.json') as f:
        state_dict = json.load(f)

    return [state_dict[x] for x in states]


def remove_blanks_and_change_to_lower(column):
    # change states to lower case
    lowered = [x.lower() for x in column]
    answ = [x.replace(' ', '-') for x in lowered]

    return answ


def create_state_city_zip(zillow):
    states = remove_blanks_and_change_to_lower(zillow['state_names'])
    cities = remove_blanks_and_change_to_lower(zillow['City'])
    zips = zillow['RegionName']

    state_cities = [i + '/' + k + '/' + z for i, k, z in zip(states, cities, zips)]
    return state_cities


def clean_zip_data(zillow, file_name):
    zillow['region'] = translate_state_to_region(zillow.State)

    zillow['state_names'] = translate_state_names(zillow.State)

    zillow['state_city_zip'] = create_state_city_zip(zillow)

    if file_name != 'Zri_AllHomesPlusMultifamily_Summary':
        # prep for merging latest month
        zillow = zillow.loc[:, ['state_city_zip', last_column_date]]

        zillow = zillow.rename(columns={last_column_date: str(file_name) + last_column_date})
    else:

        zillow = zillow.loc[:, ['state_city_zip'] + ['Date', 'SizeRank', 'Zri', 'MoM', 'QoQ', 'YoY', 'ZriRecordCnt']]
        zillow = zillow.rename(columns={'Date': str(file_name) + 'data_from'})

    return zillow

def read_write(write = False, read = True):

    path = 'http://files.zillowstatic.com/research/public/Zip/' + prefix
    extension = '.csv'
    if write:
        for name in names:
            df = pd.read_csv(path + name + extension, encoding='ISO-8859-1', index_col=False,
                             converters={'RegionName': lambda x: str(x)})
        df.to_csv('data/zillow/' + prefix + name + current_date + extension, index=False)

    if read:
        # combine all the median frames
        for idx, i in enumerate(names):
            if idx ==0:
                new_frame = pd.read_csv('data/zillow/' + prefix + i + current_date + '.csv',
                                        converters={'RegionName': lambda x: str(x)})
                new_frame = clean_zip_data(new_frame, i)
                zillow = new_frame
            else:
                new_frame = pd.read_csv('data/zillow/' + prefix + i + current_date + '.csv',
                                        converters={'RegionName': lambda x: str(x)})
                new_frame = clean_zip_data(new_frame, i)
                zillow = zillow.merge(new_frame, on='state_city_zip', how='outer')

    return zillow

zip_data = read_write(write=False, read = True)
#zip_data.to_pickle('data/final_database_frames/zillow_19-06.pkl')
