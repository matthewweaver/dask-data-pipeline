import sqlite3
from contextlib import closing
from typing import List
import dask.dataframe as dd
from us_states_abbreviations import abbreviation_to_name

name_to_abbreviation = {v: k for k, v in abbreviation_to_name.items()}


def read_files(
    filename: str, separator: str | None = None, date_columns: List[str] | None = None
) -> dd.DataFrame:
    return dd.read_csv(
        filename, sep=separator, parse_dates=date_columns, engine="python"
    )


def transform_softball(softball: dd.DataFrame) -> dd.DataFrame:
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


def create_master_dataframe(softball: dd.DataFrame, golf: dd.DataFrame) -> dd.DataFrame:
    master = dd.concat([softball, golf], ignore_index=True)
    companies = read_files("data/companies.csv")
    master_with_company_name = master.join(companies.set_index("id"), on="company_id")
    master_with_company_name = master_with_company_name.rename(
        columns={"name": "company_name"}
    )
    master_with_company_name = master_with_company_name.drop("company_id", axis=1)
    return master_with_company_name


def remove_bad_records(df: dd.DataFrame) -> tuple[dd.DataFrame, dd.DataFrame]:
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


def ingest_into_sql_db(table_name: str, df: dd.DataFrame) -> None:
    # Creates sql database file in current working directory when run first time
    with closing(sqlite3.connect("exercise.db")):
        # Ingest pandas/dask dataframes into SQL database
        df.to_sql(table_name, "sqlite:///exercise.db")


def main():
    golf = read_files(
        filename="data/unity_golf_club.csv", date_columns=["dob", "last_active"]
    )
    softball = read_files(
        filename="data/us_softball_league.tsv",
        separator="\t",
        date_columns=["date_of_birth", "last_active"],
    )
    softball = transform_softball(softball)
    softball["source_file"] = "data/us_softball_league.tsv"
    golf["source_file"] = "data/unity_golf_club.csv"
    master = create_master_dataframe(softball, golf)
    master, bad_records = remove_bad_records(master)
    print(master.head())
    print(master.info())
    ingest_into_sql_db("master", master)
    ingest_into_sql_db("bad_records", bad_records)


if __name__ == "__main__":
    main()
