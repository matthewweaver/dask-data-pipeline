# Data Engineering Project

The US Softball League has entered into an agreement with Unity Golf Club to provide a common set of membership benefits to both organizations. But there are problems: the data is not entirely compatible. To help these two fine organizations, clean up the data and provide some simple statistics.

## Part 1: Data Munging

Given two data files standardize and combine them into a common file for further analysis.

Steps:
1. Transform us_softball_league.tsv to match unity_golf_club.csv in columns and format.
    - Standardize first and last name columns.
    - Convert dates into a common format.
    - All states should be in two character abbreviation.
2. Combine the two files into one master file.
    - Indicate the source file for each record in the combined file.
3. Use companies.csv to replace `company_id` with the company name.
4. Identify suspect records.
    - Write bad records into a separate file.
    - Drop those records from the main file.

Samples:

`us_softball_league.tsv`:

| id |              name | date_of_birth | company_id | last_active | score | joined_league |     us_state |
|---:|------------------:|--------------:|-----------:|------------:|------:|--------------:|-------------:|
|  0 |   Mikayla Brennan |    11/02/1966 |          2 |  07/04/2018 |    84 |          1989 |     Illinois |
|  1 |     Thomas Holmes |    11/29/1962 |          1 |  05/15/2018 |    92 |          1972 |    Wisconsin |
|  2 |       Corey Jones |    12/20/1964 |          7 |  08/25/2018 |    47 |          2007 |   New Mexico |
|  3 |      Laura Howard |    04/26/1989 |          8 |  04/15/2018 |    76 |          1976 |   New Jersey |
|  4 | Daniel Mclaughlin |    06/19/1966 |         13 |  05/10/2018 |    56 |          1986 | Rhode Island |

`unity_golf_club.csv`:

| id |  first_name |  last_name |        dob | company_id | last_active | score | member_since | state |
|---:|------------:|-----------:|-----------:|-----------:|------------:|------:|-------------:|------:|
|  0 |      Robert | Mclaughlin | 1967/03/26 |          3 |  2018/08/25 |    57 |         2013 |    OR |
|  1 |    Brittany |     Norris | 1972/09/06 |         12 |  2018/03/29 |    73 |         1986 |    MD |
|  2 |      Sharon |    Nichols | 1971/04/19 |          7 |  2018/04/11 |    92 |         1985 |    WY |
|  3 | Christopher |       Ware | 1977/05/25 |         11 |  2018/07/20 |    74 |         2003 |    PA |
|  4 |       Kevin |      Scott | 1981/12/15 |          8 |  2018/11/20 |    42 |         1994 |    MN |

`companies.csv`:

| id |                       name |
|---:|---------------------------:|
|  0 |        Williams-Stephenson |
|  1 | Brown, Vasquez and Sanchez |
|  2 |               Keller Group |
|  3 |               Mcdonald Inc |
|  4 |                  Bruce Inc |


## Part 2: Ingestion System

Build a data model to support the data seen here and write software that will write this data to a database. For the purposes of this exercise use a SQLite3 or PostgreSQL database running locally.

> Proceed as if the dataset does not fit entirely into RAM.
