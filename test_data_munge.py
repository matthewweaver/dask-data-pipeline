from data_munge import *
import pytest
import pandas as pd
import datetime

def test_read_files():
    expected_top_row = {'first_name': 'Robert', 'last_name':'Mclaughlin', 'dob': '1967-03-26', 'company_id': 3, 'last_active': '2018-08-25', 'score': 57, 'member_since': 2013, 'state': 'OR'}
    test_df = pd.DataFrame.from_dict(expected_top_row).astype({0:str,1:str,2:datetime,3:int,4:datetime,5:int,6:datetime,7:str})
    golf = read_files(filename="unity_golf_club.csv", date_columns=['dob', 'last_active'])
    assert golf.head(1) == test_df

# def test_transform_softball():

# def test_create_master_dataframe():

# def test_remove_bad_records():

# def test_ingest_into_sql_db():

# def test_main():


# Test database persists
# res = cursor.execute("SELECT * FROM master")
# res.fetchone()