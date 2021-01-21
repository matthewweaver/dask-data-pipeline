import pytest

from data_munge import Datamunge

def test_readfile_us_softball_league():
	datamunge=Datamunge()
	df=datamunge.readfile('us_softball_league.tsv')
	assert (len(df)>=0)

def test_readfile_golf_club():
	datamunge=Datamunge()
	df=datamunge.readfile('unity_golf_club.csv')
	assert (len(df)>=0)


def test_readfile_companies():
	datamunge=Datamunge()
	df=datamunge.readfile('companies.csv')
	assert (len(df)>=0)

def test_readfile_incorrect_filename():
	datamunge=Datamunge()
	with pytest.raises(FileNotFoundError):
		df=datamunge.readfile('unity_golf_clb.csv')


def test_schema_of_two_datasets():
	datamunge=Datamunge()
	ugc_df=datamunge.readfile('unity_golf_club.csv')
	schema=ugc_df.columns
	usl_df=datamunge.readfile('us_softball_league.tsv')
	usl_df=datamunge.standardize_schema(usl_df,schema)
	assert (usl_df.columns.all()==ugc_df.columns.all())