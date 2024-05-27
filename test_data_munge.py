from data_munge import *
import pytest
import pandas as pd
import datetime

def test_read_csv():
    expected_top_row = {'first_name': ['Robert'], 'last_name': ['Mclaughlin'], 'dob': ['1967-03-26'], 'company_id': [3], 'last_active': ['2018-08-25'], 'score': [57], 'member_since': [2013], 'state': ['OR']}
    test_df = pd.DataFrame.from_dict(expected_top_row)
    test_df[["dob", "last_active"]] = test_df[["dob", "last_active"]].apply(pd.to_datetime)
    golf = read_files(filename="unity_golf_club.csv", date_columns=['dob', 'last_active'])
    pd.testing.assert_frame_equal(test_df, golf.head(1))

def test_read_tsv():
    expected_top_row = {'name': ['Mikayla Brennan'], 'date_of_birth': ['1966-11-02'], 'company_id': [2], 'last_active': ['2018-07-04'], 'score': [84], 'joined_league': [1989], 'us_state': ['Illinois']}
    test_df = pd.DataFrame.from_dict(expected_top_row)
    test_df[["date_of_birth", "last_active"]] = test_df[["date_of_birth", "last_active"]].apply(pd.to_datetime)
    softball = read_files(filename="us_softball_league.tsv", separator="\t", date_columns=['date_of_birth', 'last_active'])
    pd.testing.assert_frame_equal(test_df, softball.head(1))

def test_transform_softball():
    softball = read_files(filename="us_softball_league.tsv", separator="\t", date_columns=['date_of_birth', 'last_active'])
    softball = transform_softball(softball)
# def test_create_master_dataframe():

# def test_remove_bad_records():

# def test_ingest_into_sql_db():

# def test_main():


# Test database persists
# res = cursor.execute("SELECT * FROM master")
# res.fetchone()