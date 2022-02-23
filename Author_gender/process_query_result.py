import sqlite3 as sql
import pandas as pd
import gender_guesser.detector as gender

"""
add_gender.py collected authors gender from NB's authority register. But some entries got duplicated.
Otherwise it appears to be identical. This script removes the duplicates. Some entries are duplicated in the original data. 
"""
def convert(gen):
    male = ['male', 'mostly_male']
    female = ['female', 'mostly_female']
    na_ = ['unknown', 'andy']

    if gen in male:
        return 'm'
    elif gen in female:
        return 'f'
    elif gen in na_:
        return 'Na'

# Connect to the harvested data
conn = sql.connect("author_gender.db")

# Load table in pandas
gdb = pd.read_sql("""
SELECT * FROM author_gender ;
""", conn)

# Drop duplicates
gdb.drop_duplicates(subset='dhlabid', inplace=True)


# Export to destination db, no index
#gdb.to_sql("author_gender_dd", conn, if_exists='replace', index=False)

# Detect gender
d = gender.Detector()

df = gdb.set_index("dhlabid")

aut = df['authors'].str.split("/", expand=True).stack()
gen = df['gender'].str.split("/", expand=True).stack()

joined = aut.to_frame("authors").join(gen.to_frame("gender"))
n = joined.authors.str.split(', ', expand=True)
fnames = n[1].str.split(" ", expand=True)[0]
joined['guessed_genders'] = fnames.apply(d.get_gender)
joined['gg_gender'] = joined.guessed_genders.apply(convert)
dfn = joined.drop('guessed_genders', axis=1).rename(columns = {'gender' : 'authority_reg_gender'})
df_unstacked = dfn.unstack()

dct = {}
for c in df_unstacked.columns.get_level_values(0).unique():
    split = df_unstacked[c].fillna('')
    dct[c] = split.agg('/'.join, axis=1).str.replace('//|/$', '', regex=True)


# split = df_unstacked['authors'].fillna('')
# authors = split.agg('/'.join, axis=1).str.replace('//|/$', '', regex=True)

# split = df_unstacked['authority_reg_gender'].fillna('')
# authority_reg_gender = split.agg('/'.join, axis=1).str.replace('//|/$', '', regex=True)

# split = df_unstacked['gg_gender'].fillna('')
# gg_gender = split.agg('/'.join, axis=1).str.replace('//|/$', '', regex=True)

bigdf = pd.concat(dct, axis = 1)
bigdf.columns = ['authors', 'gender_aut_reg', 'gender_genderguesser']

df = bigdf.loc[bigdf['authors'] != '']

df.to_csv("author_gender_processed.csv")