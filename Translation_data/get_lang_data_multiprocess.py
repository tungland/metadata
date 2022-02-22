import multiprocessing as mp
from time import sleep

import glob
from pymarc import XmlHandler, parse_xml
import pandas as pd
import sqlite3 as sql
import time
import os

"""Parses translation data from nationalbibliography MARC XML. Uses multiple files. 
    
    """


class myXmlHandler(XmlHandler):
    def __init__(self, strict=False, normalize_form=None):       
       
        self.records = []
        self._record = None
        self._field = None
        self._subfield_code = None
        self._text = []
        self._strict = strict
        self.normalize_form = normalize_form
    
        
    def process_record(self, record):
        # Check if record has 041h field, i.e. "translated from"
        if record['041']:
            print("Processing records")
            if record['041'].get_subfields('h'):
                # Object MMSID
                mmsid = record["001"].data
                # Object language from controlstring
                lang = record["008"].data[35:38]
                #
                org_mmsid = None
                org_tit = None 
                org_lang = None  
                # Check if object has 765 'translation field'
                if record["765"]:
                    # 765t 'origianl title'
                    if record["765"].get_subfields('t'): 
                        org_tit = record["765"].get_subfields('t')[0] 
                    # 765w 'original mmsid'
                    if record["765"].get_subfields('w'):
                        org_mmsid = ','.join(record["765"].get_subfields('w'))
                # Join together original languages - can be multiple entries
                if record["041"]:            
                        org_lang = ','.join(record["041"].get_subfields('h'))
                
                                  
                # Serialize to pandas dataframe    
                df = pd.DataFrame([[mmsid, lang, org_lang, org_tit, org_mmsid]], columns=['mmsid', 'lang', 'org_lang', 'org_title', 'org_mmsid'])

                # Write to csv. Had some trouble getting sqlite to work with multiprocessing.
                #df.to_sql('translation_data',self.con, if_exists='append', index=False)
                df.to_csv('langs2.csv', mode='a', header=not os.path.exists('langs2.csv'), index=False)



if __name__ == "__main__":
    # Returns list of Nationalbibliography from folder path    
    tasks = glob.glob("/home/larsm/basex/nasjonalbiblio/shards3/*")
    num_workers = mp.cpu_count() 
    pool = mp.Pool(num_workers)
    xh = myXmlHandler()
    for task in tasks: 
        pool.apply_async(parse_xml, args=(task, xh))
        print(f"Finishing {task}")
    
    pool.close()
    pool.join()
    print(f"Finishing all")