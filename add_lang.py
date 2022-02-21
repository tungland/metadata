from pymarc import XmlHandler, parse_xml
import pandas as pd
import sqlite3 as sql
import time
import os

class myXmlHandler(XmlHandler):
    def __init__(self, con, strict=False, normalize_form=None):
        self.con = con
        self.records = []
        self._record = None
        self._field = None
        self._subfield_code = None
        self._text = []
        self._strict = strict
        self.normalize_form = normalize_form
        self.cur = con.cursor()
        
    def process_record(self, record):
        if record['041']:
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
                    sql
                df = pd.DataFrame([[mmsid, lang, org_lang, org_tit, org_mmsid]], columns=['mmsid', 'lang', 'org_lang', 'org_title', 'org_mmsid'])
                #return df
                # for row in df.iterrows():
                #     self.cur.execute("""
                #                  INSERT INTO translation_data VALUES (?, ?, ?, ?)
                #                  """, row[1][0], row[1][1], row[1][2], row[1][3], row[1][4])
                # #df.to_sql('translation_data',self.con, if_exists='append', index=False)
                df.to_csv('lang_41h3.csv', mode='a', header=not os.path.exists('lang.csv'), index=False)
def main():
    #path = "/home/larsm/basex/nasjonalbibliografien_root_2022.xml"
    path = "/home/larsm/basex/nasjonalbiblio/shards3/nasjonal_850001_50000.xml"
    con = sql.connect('translation_data_only041h3-smallsql2.db')
    xh = myXmlHandler(con)
    parse_xml(path, xh)


if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))