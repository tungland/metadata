import sqlite3 as sql
import pandas as pd
from autoritetsregister_oppslag import author_gender

def process_result(lst):
    if not lst:
        return "Na"
    else:
        val = lst[0][1]
        if val == '':
            return 'Na'
        elif val in ['m', 'f']:
            return val
        else:
            raise ValueError
        
import time
start_time = time.time()



conn = sql.connect("/home/larsm/my_projects/metadata/metadata_test.db")


query = "select oaiid, authors from metadata_core limit 10000000 ;"

count = 1
for chunk in pd.read_sql(query, conn, chunksize=1000):
    chunk_time = time.time()
    print(f"Processing {count}-{count + 1000}")
    res = chunk.set_index("oaiid").iloc[:, 0].str.split("/", expand=True).applymap(author_gender, na_action='ignore')
    split = res.applymap(process_result, na_action='ignore')

    split1 = split.fillna('')
    gen = split1.agg('/'.join, axis=1).str.replace('//|/$', '', regex=True).to_frame('gender')

    chunk = chunk.join(gen, on="oaiid")
    
    
    chunk.to_csv(f"authors_{count}-{count + 1000}.csv")
    count += 1000
    print("done")
    print("Chunk: --- %s seconds ---" % (time.time() - chunk_time))
    print("Total: --- %s seconds ---" % (time.time() - start_time))
    if count > 2001 : break
    
