import sqlite3
from dask.utils import tmpfile
import sqlalchemy
from contextlib import closing
import dask.dataframe as dd
from us_states_abbreviations import abbreviation_to_name

name_to_abbreviation = {v: k for k, v in abbreviation_to_name.items()}


def read_files(filename, separator=None, date_columns=None):
    return dd.read_csv(
        filename, sep=separator, parse_dates=date_columns, engine="python"
    )


def transform_softball(softball):
    # Standardise first and last name columns - only take first two values incase multiple spaces
    softball["first_name"] = softball["name"].str.split(expand=True, n=2)[0]
    softball["last_name"] = softball["name"].str.split(expand=True, n=2)[1]
    softball = softball.drop("name", axis=1)
    # Map states to two char abbv
    softball["state"] = softball["us_state"].map(
        lambda state_name: name_to_abbreviation[state_name], meta=("state_name", str)
    )
    softball = softball.drop("us_state", axis=1)
    softball = softball.rename(
        columns={"date_of_birth": "dob", "joined_league": "member_since"}
    )
    return softball


def create_master_dataframe(softball, golf):
    master = dd.concat([softball, golf], ignore_index=True)
    companies = read_files("companies.csv")
    master_with_company_name = master.join(companies.set_index("id"), on="company_id")
    master_with_company_name = master_with_company_name.rename(
        columns={"name": "company_name"}
    )
    master_with_company_name = master_with_company_name.drop("company_id", axis=1)
    return master_with_company_name


def remove_bad_records(df):
    # Create bad records dataframe
    bad_records = df.loc[
        (df["last_active"].dt.year < df["member_since"])
        | (df["member_since"] < df["dob"].dt.year)
        | (df["last_active"] < df["dob"])
    ]
    # Drop bad records from master
    df = df.loc[
        (df["last_active"].dt.year >= df["member_since"])
        | (df["member_since"] >= df["dob"].dt.year)
        | (df["last_active"] >= df["dob"])
    ]
    return df, bad_records


def ingest_into_sql_db(**dfs):
    # Creates sql database file in current working directory when run first time
    with closing(sqlite3.connect("exercise.db")) as connection:
        for df in dfs:
            # Ingest pandas/dask dataframes into SQL database
            dfs[df].to_sql(df, "sqlite:///exercise.db")


def main():
    golf = read_files(
        filename="unity_golf_club.csv", date_columns=["dob", "last_active"]
    )
    softball = read_files(
        filename="us_softball_league.tsv",
        separator="\t",
        date_columns=["date_of_birth", "last_active"],
    )
    softball = transform_softball(softball)
    softball["source_file"] = "us_softball_league.tsv"
    golf["source_file"] = "unity_golf_club.csv"
    master = create_master_dataframe(softball, golf)
    master, bad_records = remove_bad_records(master)
    print(master.head())
    print(master.info())
    ingest_into_sql_db(master=master, bad_records=bad_records)


if __name__ == "__main__":
    main()
