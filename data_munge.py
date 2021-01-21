import pandas as pd


class Datamunge:
    """ """

    def readfile(self, filename):
        if filename[-3:] == "csv":
            try:
                df = pd.read_csv(filename)
            except:
                raise FileNotFoundError

        elif filename[-3:] == "tsv":
            try:

                df = pd.read_csv(filename, sep="\t")
            except:
                raise FileNotFoundError
        else:
            raise FileNotFoundError
        return df

    def standardize_schema(self, df, schema):
        sbl_df = df["name"].str.split(" ", n=1, expand=True)
        df["first_name"] = sbl_df[0]
        df["last_name"] = sbl_df[1]
        df.drop(columns=["name"], inplace=True)
        df.rename(
            columns={
                "date_of_birth": "dob",
                "us_state": "state",
                "joined_league": "member_since",
            },
            inplace=True,
        )
        df = df[schema]
        return df

    def convert_to_date_type(self, df):
        df["dob"] = pd.to_datetime(df["dob"])
        df["last_active"] = pd.to_datetime(df["last_active"])
        return df


if __name__ == "__main__":
    datamunge = Datamunge()

    usl_df = datamunge.readfile("us_softball_league.tsv")
    ugc_df = datamunge.readfile("unity_golf_club.csv")
    companies_df = read_file("us_softball_league.csv")
    schema = ugc_df.columns
    usl_df = datamunge.rename_columns(usl_df, schema)
    usl_df = datamunge.convert_to_date_type(usl_df)
    ugc_df = datamunge.convert_to_date_type(ugc_df)
