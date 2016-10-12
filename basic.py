#/usr/lib/env python

import httplib

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

if __name__== "__main__":
    t = load_page("www.mataharimall.com","/","http")
    print t
