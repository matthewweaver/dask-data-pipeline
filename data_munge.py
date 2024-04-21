import pandas as pd
import sqlite3
from us_states_abbreviations import abbreviation_to_name
name_to_abbreviation = {v: k for k, v in abbreviation_to_name.items()}

def read_files(filename, separator=None, date_columns=None):
    return pd.read_csv(filename, sep=separator, parse_dates=date_columns)
    
def transform_softball(softball):    
    # Standardise first and last name columns - only take first two values incase multiple spaces
    softball['first_name'] = softball["name"].str.split(expand=True)[0]
    softball['last_name'] = softball["name"].str.split(expand=True)[1]
    softball.drop('name', axis=1, inplace=True)
    # Map states to two char abbv
    softball['state'] = softball['us_state'].map(lambda state_name: name_to_abbreviation[state_name])
    softball.drop('us_state', axis=1, inplace=True)
    softball.rename(columns={"date_of_birth": "dob", "joined_league": "member_since",},inplace=True)
    return softball

def remove_bad_records(df):
    # Sanity check the dates
    print(df['last_active'].agg(['min', 'max']))
    print(df['member_since'].agg(['min', 'max']))
    print(df['dob'].agg(['min', 'max']))
    # Create bad records dataframe
    bad_records = df.loc[(df['last_active'].dt.year < df['member_since']) | (df['member_since'] < df['dob'].dt.year) | (df['last_active'] < df['dob'])]
    # Drop bad records from master
    df = df.drop(bad_records.index)
    return df, bad_records

def main():
    golf = read_files(filename="unity_golf_club.csv", date_columns=['dob', 'last_active']).sort_index(axis=1)
    softball = read_files(filename="us_softball_league.tsv", separator="\t", date_columns=['date_of_birth', 'last_active'])
    softball = transform_softball(softball).sort_index(axis=1)
    softball["source_file"] = "us_softball_league.tsv"
    golf["source_file"] = "unity_golf_club.csv"
    master = pd.concat([softball, golf], ignore_index=True)
    companies = read_files("companies.csv")
    master_with_company_name = master.join(companies.set_index('id'), on='company_id')
    master_with_company_name.rename(columns={'name': 'company_name'}, inplace=True)
    master_with_company_name.drop('company_id', axis=1, inplace=True)
    master_with_company_name_removed_bad_records, bad_records = remove_bad_records(master_with_company_name)
    
    # Ingest pandas dataframes into SQL database
    df = master_with_company_name_removed_bad_records
    # Creates sql database file in current working directory
    con = sqlite3.connect("exercise.db")
    cur = con.cursor()
    df.to_sql(name='master', con=con)

    # Test database persists
    res = cur.execute("SELECT * FROM master")
    res.fetchone()

    # Use Dask to handle large datasets which don't fit in memory, and sqlite3 db file

if __name__ == "__main__":
    main()
