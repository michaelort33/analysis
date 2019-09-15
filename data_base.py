"""
This file takes the df output from preparing_scraped_data (city data) and uploads it to df table
on the air bnb database
"""
# %%
from sqlalchemy import create_engine
import pymysql
import pandas as pd


#remote_engine = create_engine("mysql+pymysql://gregen5_root:Mooose33@beta.depthfirsttraining.com/gregen5_gre_questions")
#my_con_remote = remote_engine.connect()

# local
local_engine = create_engine("mysql+pymysql://michael:Mooose1010@localhost/air_bnb")
my_con_local = local_engine.connect()

# read csv data for writing to data base
df = pd.read_csv('data/df.csv')

# Write
df.to_sql(con=local_engine, if_exists='replace', index=False, name='df')

# get csv data from data base
df = pd.read_sql('SELECT * FROM df', con=local_engine)