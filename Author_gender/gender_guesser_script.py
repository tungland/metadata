import gender_guesser.detector as gender
import pandas as pd
import sqlite3 as sql

d = gender.Detector()
conn = sql.connect("author_gender.db")

df = pd.read_sql("""
select * from author_gender_dd ;
""", conn)

df = df.set_index("dhlabid")

aut = df['authors'].str.split("/", expand=True).stack()
gen = df['gender'].str.split("/", expand=True).stack()

joined = aut.to_frame("authors").join(gen.to_frame("gender"))
n = joined.authors.str.split(', ', expand=True)