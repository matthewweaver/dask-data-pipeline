import pandas as pd
from us_states_abbreviations import abbreviation_to_name
name_to_abbreviation = {v: k for k, v in abbreviation_to_name.items()}

def read_files():
    golf = pd.read_csv("unity_golf_club.csv", parse_dates=["dob"])
    softball = pd.read_csv("us_softball_league.tsv", sep="\t", parse_dates=["date_of_birth"])
    # Only take first two values incase multiple spaces
    softball['first_name'] = softball["name"].str.split(expand=True)[0]
    softball['last_name'] = softball["name"].str.split(expand=True)[1]
    softball = softball.drop('name', axis=1)
    softball['us_state'] = softball['us_state'].map(lambda state_name: name_to_abbreviation[state_name])