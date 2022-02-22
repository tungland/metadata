import sqlite3 as sql
import pandas as pd
from autoritetsregister_oppslag import author_gender
import time

"""
Uses `author_gender` to find the gender of authors from the DHLAB 
metadata DB. 
OBS: Generates some duplicates.
"""


def process_result(lst):
    """
    Processes contents of the MARC gender result.
    """
    if not lst:
        return "Na"
    else:
        val = lst[0][1]
        if val == '':
            return 'Na'
        elif val in ['m', 'f']:
            return val
        else:
            with open("nonstandard_vals.txt", "a") as f:
                f.write(val)
                f.write('\n')
            return val
        
def parse_author(con_in, con_out):
    """
    Parses the authors.
    """

    query = "select dhlabid, oaiid, authors from metadata_core where doctype = 'digibok' ; "
    start_time = time.time()
    count = 1
    for chunk in pd.read_sql(query, con_in, chunksize=10000):
        chunk_time = time.time()
        print(f"Processing {count}-{count + 10000}")
        res = chunk.set_index("oaiid").iloc[:, 1].str.split("/", expand=True)
        aut = res.applymap(author_gender, na_action='ignore')
        split = aut.applymap(process_result, na_action='ignore')

        split1 = split.fillna('')
        gen = split1.agg('/'.join, axis=1).str.replace('//|/$', '', regex=True).to_frame('gender')

        chunk = chunk.join(gen, on="oaiid")
        
        
        #chunk.to_csv(f"authors_{count}-{count + 10000}.csv")
        chunk.to_sql('author_gender', con_out, if_exists="append", index=False)
        count += 10000
        print("done")
        print("Chunk: --- %s seconds ---" % (time.time() - chunk_time))
        print("Total: --- %s seconds ---" % (time.time() - start_time))
        #if count > 2001 : break
    
def main():
    conn = sql.connect("/mnt/disk2/larsm/metadata/metadata_test.db")
    out_con = sql.connect("author_gender.db")
    parse_author(conn, out_con)

if __name__ == "__main__":
    main()