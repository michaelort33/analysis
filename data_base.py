"""
This file takes the df output from preparing_scraped_data (city data) and uploads it to df table
on the air bnb database
"""
from sqlalchemy import create_engine
import pymysql
import pandas as pd


#remote_engine = create_engine("mysql+pymysql://gregen5_root:Mooose33@beta.depthfirsttraining.com/gregen5_gre_questions")
#my_con_remote = remote_engine.connect()

# local
#local_engine = create_engine("mysql+pymysql://michael:Mooose1010@localhost/air_bnb")
local_engine = create_engine("mysql+pymysql://uszv3z4nweejr:Mooose33@ortpropertygroup.com/db5eb9d5re9s6g")
my_con_local = local_engine.connect()

# read csv data for writing to data base
df = pd.read_pickle('../housing_data/data/final_database_frames/zip_v1.pkl')

# Write
df.to_sql(con=local_engine, if_exists='replace', index=False, name='df')

# get csv data from data base
df = pd.read_sql('SELECT * FROM df', con=local_engine)

avoid = ['active_listing_tics_2', 'monthly_revenue', 'nightly_revenue','monthly_occupancy','end_points_monthly_revenue','end_points_nightly_revenue','end_points_monthly_occupancy']

don_avoid = [i for i in df.columns.values if i not in avoid]
don_avoid