import sqlite3 as sql
import pandas as pd

"""
add_gender.py collected authors gender from NB's authority register. But some entries got duplicated.
Otherwise it appears to be identical. This script removes the duplicates.
"""

# Connect to the harvested data
conn = sql.connect("/mnt/disk2/larsm/metadata/add_gender/author_gender.db")
# Connect to destionation
#aut_conn = sql.connect("/mnt/disk2/larsm/metadata/metadata_test.db")
#aut_conn = sql.connect("/mnt/disk1/metadata/metadata.db")
aut_conn = sql.connect("/mnt/disk2/larsm/metadata/metadata_test.db")
# Load table in pandas
gdb = pd.read_sql("""
SELECT * FROM author_gender ;
""", conn)

# Drop duplicates
gdb.drop_duplicates(inplace=True)


# Export to destination db, no index
gdb.to_sql("author_gender", aut_conn, index=False)