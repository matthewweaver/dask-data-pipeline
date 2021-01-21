import pytest

from data_munge import Datamunge


def test_readfile_us_softball_league():
    datamunge = Datamunge()
    df = datamunge.readfile("us_softball_league.tsv")
    assert len(df) >= 0


def test_readfile_golf_club():
    datamunge = Datamunge()
    df = datamunge.readfile("unity_golf_club.csv")
    assert len(df) >= 0


def test_readfile_companies():
    datamunge = Datamunge()
    df = datamunge.readfile("companies.csv")
    assert len(df) >= 0


def test_readfile_incorrect_filename():
    datamunge = Datamunge()
    with pytest.raises(FileNotFoundError):
        df = datamunge.readfile("unity_golf_clb.csv")


def test_schema_of_two_datasets():
    datamunge = Datamunge()
    ugc_df = datamunge.readfile("unity_golf_club.csv")
    schema = ugc_df.columns
    usl_df = datamunge.readfile("us_softball_league.tsv")
    usl_df = datamunge.standardize_schema(usl_df, schema)
    assert usl_df.columns.all() == ugc_df.columns.all()


def test_name_splitting():
    datamunge = Datamunge()
    ugc_df = datamunge.readfile("unity_golf_club.csv")
    schema = ugc_df.columns
    usl_df_1 = datamunge.readfile("us_softball_league.tsv")
    usl_df = datamunge.standardize_schema(usl_df_1, schema)
    # assert (usl_df_1["name"].iloc[0]=="Mikayla Brennan")
    assert "Mikayla Brennan" == (
        usl_df["first_name"].iloc[0] + " " + usl_df["last_name"].iloc[0]
    )


def test_convert_to_date_type():
    datamunge = Datamunge()
    ugc_df = datamunge.readfile("unity_golf_club.csv")
    schema = ugc_df.columns
    usl_df_1 = datamunge.readfile("us_softball_league.tsv")
    usl_df = datamunge.standardize_schema(usl_df_1, schema)
    usl_df = datamunge.convert_to_date_type(usl_df)
    ugc_df = datamunge.convert_to_date_type(ugc_df)
    assert usl_df["dob"].dtypes == "<M8[ns]"
    assert usl_df["last_active"].dtypes == "<M8[ns]"
    assert ugc_df["dob"].dtypes == "<M8[ns]"
    assert ugc_df["last_active"].dtypes == "<M8[ns]"
