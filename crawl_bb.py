#/usr/lib/env python

import httplib
from bs4 import BeautifulSoup
import MySQLdb
from urlparse import urlparse
import json
from pprint import pprint
import sys

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

def insertDB_product(product_id, product_title, old_price, new_price,
discount, image, link_page):
    # Open database connection
    db = MySQLdb.connect(serverdb, user, passw, dba )
    #fields = { `db_id`, `attribute_102_value`, `attribute_103_value`, `average_rating`, `badge_id`, `brand_name`, `category_ids`, `category_names`, `cicil_price`, `cod_enabled`, `created_time`, `discount`, `effective_price`, `effective_price_origin`, `gtm_detail_product`, `id`, `images`, `is_new`, `moderation_time`, `normal_price`, `normal_price_origin`, `path`, `product_sku`, `stock_available`, `store`, `store_id`, `store_name`, `store_url`, `title`, `variant_sku`, `update_time`}
    # prepare a cursor object using cursor() method
    cursor = db.cursor()


    # Prepare SQL query to INSERT a record into the database.
    #print (json.dumps(data, sort_keys=True, indent=4, separators=(',', ': ')))
    false = False
    true = True
    sql_v = "id, product_title, old_price, new_price, discount, image, link_page"
    sql_val = """\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\"""".format(product_id,
    product_title, old_price, new_price, discount, image, link_page)


    #print (brand_name)
    sql = """REPLACE INTO `t_product_bb`({0}) VALUES ({1})""".format(sql_v, sql_val)
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

def load_page_product(host, url, scheme):
    try:
        t = load_page(host, url,scheme)
        #print t
        soup = BeautifulSoup(t, 'html.parser')
        #print soup
        t=soup.find_all('div',attrs={"class" : "single-productset"})
        #print t
        for d in t:
                #for x in st:
            if(d!=None):
                #d.text
                #print str(d)
                ss = BeautifulSoup(str(d),'html.parser')
                product=ss.find('div',attrs={"class":"grid-custom columns"})
                product_id = product.get('id')
                product_title = ss.find('div', attrs={"class":"product-title"})
                product_title = product_title.text.strip()
                old_price = ss.find('span', attrs={"class":"old-price-text"})
                if(old_price!=None):
                    old_price = old_price.text.strip()
                else:
                    old_price = "None"
                new_price = ss.find('span', attrs={"class":"new-price-text"})
                if(new_price!=None):
                    new_price = new_price.text.strip()
                else:
                    new_price = "None"
                discount = ss.find('div', attrs={"class":"discount"})
                if(discount!=None):
                    discount = discount.text.strip()
                else:
                    discount = "None"
                image = ss.find('img')
                image = image.get('src')
                link_page = ss.find('a')
                link_page = link_page.get('href')

                insertDB_product (product_id, product_title, old_price, new_price,
                discount, image, link_page)
                #tt = s.find_all('div', attrs={"class":"grid-custom"})
                #print d.get('class'), "=", d.text
            #break
            #print (json.dumps(y, sort_keys=True, indent=4, separators=(',', ': ')))

            #break
    except Exception, e :
        print e
        pass
    #print(t)


def crawllink():
    links =  load_link_from_db("t_crawl_bb")
    #links = [["https://www.blibli.com/"]]
    i=0
    if(links!=None):
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
    #links = [["https://www.blibli.com/handphone-tablet/54593"]]
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
                load_page_product(host, url_link, scheme)
                #break
                #print '',
                #load_page_link(host, url_link, scheme)
                #load_page_product_mm(host, url_link, scheme)
            #domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
            #print domain
    except Exception, e:
        print "crawlproduct: ", e # coding=utf-8
        pass

if __name__== "__main__":
    if (len(sys.argv)>2):
        if(sys.argv[2]=="link"):
            crawllink()
        if(sys.argv[2]=="product"):
            crawlproduct()
