import requests 
from bs4 import BeautifulSoup
import sys
import re


def author_gender(person):
    """ 
        Find five of the records matching input parameter person.
        Return value is a 3-tuple with name in standard format, gender and life-span.
        Only returns a value if the characters in person is a subset of the characters in a name found in bibsys.
        assumes
            import requests 
            from bs4 import BeautifulSoup
            import sys
            import re
        input
            person a string
        output
            (name string, gender char, birth-death as string)
        
    """
    # remove punctuation characters - appears to be a problem for bibsys or ...
    person = re.sub('[^(\w|\s)]','', person)

    #print(person)
    
    # set up request parameter string

    req = "https://authority.bibsys.no/authority/rest/sru?operation=searchRetrieve&query={author}&startRecord=1&maximumRecords=5&recordPacking=xml&recordSchema=marcxchange" 
    
    # res is the return value, will contain a list of tuples if anything.
    res = []

    # issue request"https://authority.bibsys.no/authority/rest/sru?operation=searchRetrieve&query={author}&startRecord=1&maximumRecords=5&recordPacking=xml&recordSchema=marcxchange" 
    r = requests.get(req.format(author = person))

    
    # if everything is ok, go ahead and pick out the information
    if r.status_code == 200:
        # extract character symbols from person, used below to compare
        person_char = set(person.lower())
        #print(person_char)
        
        soup = BeautifulSoup(r.text, 'lxml')

        #name recides in tag 100, gender in tag 375
        # loop through all query matches
        for n in soup.find_all("srw:recorddata"):
            
            # initialise the components of the return tuple
            name = ""
            year = ""
            gender = ""
            
            # then try to extract som useful information
            namedata = n.find("marc:datafield",{'tag':100})
            if not namedata is None: 
                name_n = namedata.find("marc:subfield", {'code':'a'})
                if not name_n is None:
                    name = name_n.text
                year_n = namedata.find("marc:subfield", {'code':'d'})
                if not year_n is None:
                    year = year_n.text

            genderdata = n.find("marc:datafield", {'tag':375})
            if not genderdata is None:
                gender_n = genderdata.find("marc:subfield", {'code':'a'})
                if not gender_n is None:
                    gender = gender_n.text

            # Pseudonyms and other names for person related to query can get returned - retain only those 
            # that match the query here implemented as a subset relation. 
            # Bibsys will not match ó to o for example. If it does, this has to be replace by
            # some form of jaccard formula.

            if person_char.issubset(set(name.lower())):
                res.append((name, gender, year))
            
    return res