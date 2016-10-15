#/usr/lib/env python

import httplib
from bs4 import BeautifulSoup
import MySQLdb
from urlparse import urlparse
import json
from pprint import pprint

user = "root"
passw = "lenovo"
serverdb = "localhost"
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
    db = MySQLdb.connect(serverdb, user, passw, dba )

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
    db = MySQLdb.connect(serverdb, user, passw, dba )
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

def insertDB_product(data):
    # Open database connection
    db = MySQLdb.connect(serverdb, user, passw, dba )

    # prepare a cursor object using cursor() method
    cursor = db.cursor()


    # Prepare SQL query to INSERT a record into the database.
    #print (json.dumps(data, sort_keys=True, indent=4, separators=(',', ': ')))
    false = False
    true = True
    sql_v = ""
    sql_val = ""
    for key in data.keys():
        #print key, "=", data.get(key)
        exec("{0}={1}".format(key, json.dumps(data.get(key))))
        #print key, type(eval(key))
        if key in locals():
            if(sql_v==""):
                sql_v = "{0}".format(key)
                sql_val = "{0}".format(json.dumps(data.get(key)))
            else:
                sql_v = "{0}, {1}".format(sql_v, key)
                if((type(eval(key)) is list) or (type(eval(key)) is dict)   ):
                    sql_val = "{0}, \"{1}\"".format(sql_val, str(data.get(key)).replace('\r', '').replace("\"","'"))
                elif( (type(eval(key)) is str) or (type(eval(key)) is bool)):
                    sql_val = "{0}, \"{1}\"".format(sql_val, data.get(key))
                else:
                    sql_val = "{0}, {1}".format(sql_val, data.get(key))

    #print (brand_name)
    sql = """REPLACE INTO `t_product_mm`({0}) VALUES ({1})""".format(sql_v, sql_val)
    #sql = """REPLACE INTO `t_product_mm`(%s) VALUES (%s)"""

    #print sql_v
    #print sql_val
    #pprint (sql)
    try:
       # Execute the SQL command
       cursor.execute(sql)
       # Commit your changes in the database
       db.commit()
       #print "",
    except Exception, e:
       #print sql
       print e
       # Rollback in case there is any error
       db.rollback()

    # disconnect from server
    db.close()

def load_page_product_mm(host, url, scheme):
    t = load_page(host, url,scheme)
    #print t
    st = t.find("var productObj = ")
    en = t.find("[position].gtm_detail_product;", st)
    t = t[st + len("var productObj = "): en]
    x = json.loads(t)
    for y in x:
        #print (json.dumps(y, sort_keys=True, indent=4, separators=(',', ': ')))
        insertDB_product(y)
        #break

    #print(t)


if __name__== "__main__":
    links =  load_link_from_db()
    #links = [[1,"https://fashion.mataharimall.com/p-312/sepatu-pria"]]
    i=0
    total = len(links)
    try:
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
            if(url_link!="#"):
                #load_page_link(host, url_link, scheme)
                load_page_product_mm(host, url_link, scheme)
            #domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
            #print domain
    except Exception, e:
        print e # coding=utf-8
        pass
