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
        #print host, url, scheme
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        params = ""
        if (scheme=="https"):
            c = httplib.HTTPSConnection(host)
        elif (scheme=="http"):
            c = httplib.HTTPConnection(host)
        c.request ("GET", url, params, headers)
        response = c.getresponse()
        data = response.read()
    except Exception, e:
        print "load_page:", e
    return data

def insertDB_link(tbl, url):
    # Open database connection
    db = MySQLdb.connect(serverdb, user, passw, dba )

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # Prepare SQL query to INSERT a record into the database.
    sql = """INSERT INTO %s (url, add_time)
             VALUES ('%s', NOW())""" % (tbl, url,)
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

def load_link_from_db(tbl):
    # Open database connection
    db = MySQLdb.connect(serverdb, user, passw, dba )
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    # Prepare SQL query to INSERT a record into the database.
    sql = "SELECT url FROM %s group by URL" % (tbl)
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

def load_page_link(tbl, host, url, scheme):
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
        insertDB_link(tbl, url)

def insertDB_product(data):
    # Open database connection
    db = MySQLdb.connect(serverdb, user, passw, dba )
    fields = { `db_id`, `attribute_102_value`, `attribute_103_value`, `average_rating`, `badge_id`, `brand_name`, `category_ids`, `category_names`, `cicil_price`, `cod_enabled`, `created_time`, `discount`, `effective_price`, `effective_price_origin`, `gtm_detail_product`, `id`, `images`, `is_new`, `moderation_time`, `normal_price`, `normal_price_origin`, `path`, `product_sku`, `stock_available`, `store`, `store_id`, `store_name`, `store_url`, `title`, `variant_sku`, `update_time`}
    # prepare a cursor object using cursor() method
    cursor = db.cursor()


    # Prepare SQL query to INSERT a record into the database.
    #print (json.dumps(data, sort_keys=True, indent=4, separators=(',', ': ')))
    false = False
    true = True
    sql_v = ""
    sql_val = ""
    for key in data.keys():
        if(key in fields):
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
                    elif( (type(eval(key)) is str) ):
                        sql_val = "{0}, \"{1}\"".format(sql_val, str(data.get(key)).replace('\r', '').replace("\"","'"))
                    elif((type(eval(key)) is bool)):
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
    try:
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
    except:
        pass
    #print(t)


def crawllink():
    links =  load_link_from_db("t_crawl_bb")
    #links = [["https://www.blibli.com/"]]
    i=0
    total = len(links)
    try:
        for url in links:
            #print (url[1])
            parsed_uri = urlparse(url[0])
            scheme = parsed_uri.scheme
            if(scheme==""):
                scheme = "https"
            if(parsed_uri.netloc==""):
                host = "www.blibli.com"
            else:
                host = parsed_uri.netloc
            url_link = parsed_uri.path
            i+=1
            print i, "of", total, scheme, host, url_link
            if(url_link!="#"):
                load_page_link("t_crawl_bb", host, url_link, scheme)
                #load_page_product_mm(host, url_link, scheme)
            #domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
            #print domain
    except Exception, e:
        print "crawllink:", e # coding=utf-8
        pass

def crawlproduct():
    links =  load_link_from_db("t_crawl_bb")
    #links = [[1,"https://fashion.mataharimall.com/p-312/sepatu-pria"]]
    i=0
    total = len(links)
    try:
        for url in links:
            #print (url[1])
            parsed_uri = urlparse(url[0])
            scheme = parsed_uri.scheme
            if(scheme==""):
                scheme = "https"
            if(parsed_uri.netloc==""):
                host = "www.blibli.com"
            else:
                host = parsed_uri.netloc
            url_link = parsed_uri.path
            i+=1
            print i, "of", total, scheme, host, url_link
            if(url_link!="#"):
                #new load product page need for blibli
                print '',
                #load_page_link(host, url_link, scheme)
                #load_page_product_mm(host, url_link, scheme)
            #domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
            #print domain
    except Exception, e:
        print "crawlproduct: ", e # coding=utf-8
        pass

if __name__== "__main__":
    crawllink()
