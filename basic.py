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

def load_link_from_db():
    # Open database connection
    db = MySQLdb.connect(host, user, passw, dba )
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    # Prepare SQL query to INSERT a record into the database.
    sql = "SELECT * FROM t_crawllinks"
    result = None
    try:
       # Execute the SQL command
       cursor.execute(sql)
       # Fetch all the rows in a list of lists.
       results = cursor.fetchall()
    except:
       print "Error: unable to fecth data"

    # disconnect from server
    db.close()
    return results

def load_page_link(host, url, scheme):
    t = load_page(host, url,scheme)
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

if __name__== "__main__":
    links =  load_link_from_db()
    i=0
    total = len(links)
    for url in links:
        #print (url[1])
        parsed_uri = urlparse(url[1])
        scheme = parsed_uri.scheme
        if(scheme==""):
            scheme = "https"
        if(parsed_uri.netloc==""):
            host = "www.mataharimall.com"
        else:
            host = parsed_uri.netloc
        url_link = parsed_uri.path
        i+=1
        print i, "of", total, scheme, host, url_link
        load_page_link(host, url_link, scheme)
        #domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
        #print domain
