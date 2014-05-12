xcrawler
========

a light framework of crawler with gevent, requests, leveldb

Features
========
1. multiple crawling task, use coroutine of gevent;
2. a url pool to manage urls (downloaded or to download), use leveldb;
3. a proxy pool to avoid blocked by the site while quering very frequently;

Installation
========
sudo python setup.py install


Usage
========
Sorry for no docs right now, just see the examples code in example/
