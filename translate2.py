import multiprocessing as mp
from time import sleep

import glob
from pymarc import XmlHandler, parse_xml, map_xml
import pandas as pd
import sqlite3 as sql
import time
import os

def parse_func(record) :
     if record['041']:
         print('41 found, processing record')
         if record['041'].get_subfields('h'):
                mmsid = record["001"].data
                lang = record["008"].data[35:38]
                org_mmsid = None 
        
                if record["765"]:
                    
                    if record["765"].get_subfields('t'): 
                        org_tit = record["765"].get_subfields('t')[0]
                    else:
                        org_tit = None
                    if record["765"].get_subfields('w'):
                        org_mmsid = ','.join(record["765"].get_subfields('w'))
                    else:
                        org_mmsid = None
                    
                else:
                    org_tit = None         
                
                if record["041"]:            
                        org_lang = ','.join(record["041"].get_subfields('h'))
                else:
                    pass                    
                    
                df = pd.DataFrame([[mmsid, lang, org_lang, org_tit, org_mmsid]], columns=['mmsid', 'lang', 'org_lang', 'org_title', 'org_mmsid'])
              
                df.to_sql('translation_data', sql.connect('translation_data_mp.db'), if_exists='append', index=False)
                print("Record processed")
                

if __name__ == "__main__":
    num_workers = mp.cpu_count()  
    tasks = glob.glob("/home/larsm/basex/nasjonalbiblio/shards3/*")

    pool = mp.Pool(num_workers)
    
    #con = sql.connect('translation_data_mp.db')
        
    for task in tasks[:2]:
        print(f"Starting {task}")
       
        #map_xml(parse_func, task)
        pool.apply_async(map_xml, args = (parse_func, task))
        print(f"Finishing {task}")
    pool.close()
    pool.join()