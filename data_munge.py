import pandas as pd
from us_states_abbreviations import abbreviation_to_name
name_to_abbreviation = {v: k for k, v in abbreviation_to_name.items()}

def read_files(filename, separator=None, date_columns=None):
    return pd.read_csv(filename, sep=separator, parse_dates=date_columns)
    # golf = pd.read_csv("unity_golf_club.csv", parse_dates=["dob", "last_active"])
    # softball = pd.read_csv("us_softball_league.tsv", sep="\t", parse_dates=["date_of_birth", "last_active"])
    
def transform_softball(softball):    
    # Standardise first and last name columns - only take first two values incase multiple spaces
    softball['first_name'] = softball["name"].str.split(expand=True)[0]
    softball['last_name'] = softball["name"].str.split(expand=True)[1]
    softball.drop('name', axis=1, inplace=True)
    # Map states to two char abbv
    softball['state'] = softball['us_state'].map(lambda state_name: name_to_abbreviation[state_name])
    softball.drop('us_state', axis=1, inplace=True)
    softball.rename(
            columns={
                "date_of_birth": "dob",
                "joined_league": "member_since",
            },
            inplace=True
        )
    return softball

def main():
    golf = read_files("unity_golf_club.csv", "dob", "last_active")
    softball = read_files(filename="us_softball_league.tsv", separator="\t", date_columns=['date_of_birth', 'last_active'])
    softball = transform_softball(softball)
    softball.sort_index(axis=1, inplace=True)
    golf.sort_index(axis=1, inplace=True)
    softball["source_file"] = "us_softball_league.tsv"
    golf["source_file"] = "unity_golf_club.csv"
    master = pd.concat([softball, golf])


if __name__ == "__main__":
    main()
