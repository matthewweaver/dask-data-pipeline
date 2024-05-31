from data_munge import *
import dask.dataframe as dd
from dask.dataframe.utils import assert_eq
import pandas as pd

pd.set_option("display.max_columns", None)


def test_read_csv():
    expected_top_row = {
        "first_name": ["Robert"],
        "last_name": ["Mclaughlin"],
        "dob": ["1967-03-26"],
        "company_id": [3],
        "last_active": ["2018-08-25"],
        "score": [57],
        "member_since": [2013],
        "state": ["OR"],
    }
    df = pd.DataFrame(data=expected_top_row)
    df[["dob", "last_active"]] = df[["dob", "last_active"]].apply(pd.to_datetime)
    test_df = dd.from_pandas(
        df,
        npartitions=2,
    )
    golf = read_files(
        filename="data/unity_golf_club.csv", date_columns=["dob", "last_active"]
    )
    assert_eq(test_df, golf.head(1))


def test_read_tsv():
    expected_top_row = {
        "name": ["Mikayla Brennan"],
        "date_of_birth": ["1966-11-02"],
        "company_id": [2],
        "last_active": ["2018-07-04"],
        "score": [84],
        "joined_league": [1989],
        "us_state": ["Illinois"],
    }
    df = pd.DataFrame(data=expected_top_row)
    df[["date_of_birth", "last_active"]] = df[["date_of_birth", "last_active"]].apply(
        pd.to_datetime
    )
    test_df = dd.from_pandas(
        df,
        npartitions=2,
    )
    softball = read_files(
        filename="data/us_softball_league.tsv",
        separator="\t",
        date_columns=["date_of_birth", "last_active"],
    )
    assert_eq(test_df, softball.head(1))


def test_transform_softball():
    expected_top_row = {
        "dob": ["1966-11-02"],
        "company_id": [2],
        "last_active": ["2018-07-04"],
        "score": [84],
        "member_since": [1989],
        "first_name": ["Mikayla"],
        "last_name": ["Brennan"],
        "state": ["IL"],
    }
    df = pd.DataFrame(data=expected_top_row)
    df[["dob", "last_active"]] = df[["dob", "last_active"]].apply(pd.to_datetime)
    test_df = dd.from_pandas(
        df,
        npartitions=2,
    )
    softball = read_files(
        filename="data/us_softball_league.tsv",
        separator="\t",
        date_columns=["date_of_birth", "last_active"],
    )
    softball = transform_softball(softball)
    assert_eq(test_df, softball.head(1))


def test_create_master_dataframe():
    expected_top_row = {
        "dob": ["1966-11-02"],
        "last_active": ["2018-07-04"],
        "score": [84],
        "member_since": [1989],
        "first_name": ["Mikayla"],
        "last_name": ["Brennan"],
        "state": ["IL"],
        "source_file": "data/us_softball_league.tsv",
        "company_name": "Keller Group",
    }
    df = pd.DataFrame(data=expected_top_row)
    df[["dob", "last_active"]] = df[["dob", "last_active"]].apply(pd.to_datetime)
    test_df = dd.from_pandas(
        df,
        npartitions=2,
    )
    softball = read_files(
        filename="data/us_softball_league.tsv",
        separator="\t",
        date_columns=["date_of_birth", "last_active"],
    )
    softball = transform_softball(softball)
    golf = read_files(
        filename="data/unity_golf_club.csv", date_columns=["dob", "last_active"]
    )
    softball["source_file"] = "data/us_softball_league.tsv"
    golf["source_file"] = "data/unity_golf_club.csv"
    master = create_master_dataframe(softball, golf)
    assert_eq(test_df, master.head(1))


def test_remove_bad_records():
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
    assert len(master) == 16118 and len(bad_records) == 3370


def test_ingest_into_sql_db():
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
    ingest_into_sql_db("master", master)
    ingest_into_sql_db("bad_records", bad_records)
    with closing(sqlite3.connect("exercise.db")) as connection:
        with closing(connection.cursor()) as cursor:
            master_row = cursor.execute("SELECT * FROM master LIMIT 1").fetchall()
            assert master_row == [
                (
                    0,
                    "1966-11-02 00:00:00.000000",
                    "2018-07-04 00:00:00.000000",
                    84,
                    1989,
                    "Mikayla",
                    "Brennan",
                    "IL",
                    "data/us_softball_league.tsv",
                    "Keller Group",
                )
            ]
            assert (
                cursor.execute("SELECT COUNT(*) FROM master").fetchall()[0][0] == 16118
            )
            bad_records_row = cursor.execute(
                "SELECT * FROM bad_records LIMIT 1"
            ).fetchall()
            assert bad_records_row == [
                (
                    3,
                    "1989-04-26 00:00:00.000000",
                    "2018-04-15 00:00:00.000000",
                    76,
                    1976,
                    "Laura",
                    "Howard",
                    "NJ",
                    "data/us_softball_league.tsv",
                    "Smith, Torres and Matthews",
                )
            ]
            assert (
                cursor.execute("SELECT COUNT(*) FROM bad_records").fetchall()[0][0]
                == 3370
            )
