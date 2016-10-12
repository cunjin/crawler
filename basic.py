#/usr/lib/env python

import httplib
from bs4 import BeautifulSoup
import MySQLdb
from urlparse import urlparse


user = "root"
passw = "lenovo"
host = "localhost"
dba = "crawldb"


def load_page(host, url, scheme):
    try:
        data = None
        if (scheme=="https"):
            c = httplib.HTTPSConnection(host)
        elif (scheme=="http"):
            c = httplib.HTTPConnection(host)
        c.request ("GET", url)
        response = c.getresponse()
        data = response.read()
    except Exception, e:
        print e
    return data

def insertDB_link(url):
    # Open database connection
    db = MySQLdb.connect(host, user, passw, dba )

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database.
    sql = """INSERT INTO t_crawllinks (url, add_time)
             VALUES ('%s', NOW())""" % (url,)
    try:
       # Execute the SQL command
       cursor.execute(sql)
       # Commit your changes in the database
       db.commit()
    except Exception, e:
       print sql
       print e
       # Rollback in case there is any error
       db.rollback()

    # disconnect from server
    db.close()

if __name__== "__main__":
    t = load_page("www.mataharimall.com","/","https")
    #print(t)
    soup = BeautifulSoup(t, 'html.parser')
    t=soup.find_all('a')
    url=None
    for link in t:
        rel = link.get('rel')
        if(rel!=None):
            if(''.join(rel)!='nofollow'): #for str in rel:
                url= (link.get('href'))
        else:
            url=(link.get('href'))
        insertDB_link(url)
