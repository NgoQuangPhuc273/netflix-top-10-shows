import psycopg2
import pandas as pd

# # Note 1: Create a database
def connect_database():
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=Neverland1")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute("DROP DATABASE IF EXISTS netflix")
    cur.execute("CREATE DATABASE netflix")

    conn.close()

    conn = psycopg2.connect("host=127.0.0.1 dbname=netflix user=postgres password=Neverland1")
    # conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn   

# def drop_table(cur, conn):
#     for query in drop_table_queries:
#         cur.execute(query)
#         conn.commit

# def create_tables(cur, conn):
#     for query in create_table_queries:
#         cur.execute(query)
#         conn.commit()
        

netflix_countries = pd.read_csv("data/all-weeks-countries.csv")
netflix_global = pd.read_csv("data/all-weeks-global.csv")
netflix_popular = pd.read_csv("data/most-popular.csv")

netflix_countries_short = netflix_countries[['country_name', 'category', 'weekly_rank', 'show_title', 'cumulative_weeks_in_top_10']]

netflix_global_short = netflix_global[['category', 'weekly_rank','show_title', 'weekly_hours_viewed', 'cumulative_weeks_in_top_10']]

netflix_popular_short = netflix_popular[['category', 'rank', 'show_title', 'hours_viewed_first_28_days' ]]


# print(netflix_popular.head(10))
# print(netflix_global)
# print(netflix_popular.columns)

cur, conn = connect_database()

netflix_countries_create = ("""CREATE TABLE IF NOT EXISTS netflix_countries(
country_name VARCHAR,
category VARCHAR,
weekly_rank VARCHAR, 
show_title VARCHAR,
cumulative_weeks_in_top_10 VARCHAR
)""")

netflix_global_create = ("""CREATE TABLE IF NOT EXISTS netflix_global(
category VARCHAR, 
weekly_rank VARCHAR,
show_title VARCHAR,
weekly_hours_viewed VARCHAR, 
cumulative_weeks_in_top_10 VARCHAR
)""")

netflix_popular_create = ("""CREATE TABLE IF NOT EXISTS netflix_popular(
category VARCHAR,
rank VARCHAR, 
show_title VARCHAR,
hours_viewed_first_28_days VARCHAR
)""")

cur.execute(netflix_countries_create)
cur.execute(netflix_global_create)
cur.execute(netflix_popular_create)

conn.commit()

netflix_countries_insert = ("""INSERT INTO netflix_countries(
country_name, 
category, 
weekly_rank, 
show_title,
cumulative_weeks_in_top_10)
VALUES (%s,%s,%s,%s,%s)
""")

netflix_global_insert = ("""INSERT INTO netflix_global(
category, 
weekly_rank, 
show_title,
weekly_hours_viewed, 
cumulative_weeks_in_top_10)
VALUES (%s,%s,%s,%s,%s)
""")

netflix_popular_insert = ("""INSERT INTO netflix_popular(
show_title,
rank, 
category, 
hours_viewed_first_28_days)
VALUES (%s,%s,%s,%s)
""")

for i, row in netflix_countries_short.iterrows():
    cur.execute(netflix_countries_insert, list(row))

for i, row in netflix_global_short.iterrows():
    cur.execute(netflix_global_insert, list(row))

for i, row in netflix_popular_short.iterrows():
    cur.execute(netflix_popular_insert, list(row))

conn.commit()

