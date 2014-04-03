#!/usr/bin/env python
#encoding: UTF-8
"""
    url pool to manage urls for xcrawler
    by veelion@ebuinfo.com
    Copyright © 2013 - 2014 Ebuinfo. All Rights Reserved.
"""
import time
import random
import leveldb
import urlparse
from url_normalize import url_normalize

RED = '\x1b[31m'
GRE = '\x1b[32m'
BRO = '\x1b[33m'
BLU = '\x1b[34m'
PUR = '\x1b[35m'
CYA = '\x1b[36m'
WHI = '\x1b[37m'
NOR = '\x1b[0m'


class UrlPool(object):
    '''
    to pop() a url whith different host each time
    '''

    _URL_TASK = 'task'
    _URL_DONE = 'done'
    _URL_BAD  = 'bad'
    _URL_NONE = False
    def __init__(self, urlindex_file="", urls=None,
                 load_bad_url=False,
                 is_good_link=None):
        if not urlindex_file:
            urlindex_file = 'xcrawler.url.idx'
        if not is_good_link:
            import sys
            print 'no is_good_link function!!!!'
            sys.exit()
        print is_good_link
        self._urlindex = leveldb.LevelDB(urlindex_file)
        self._pool = {} # host: [urls]
        self._hosts_pop_recently = {}
        self.url_count = 0
        if urls:
            self.url_count += len(urls)
            self.addmany(urls, always=True)
        self._load_from_url_index(load_bad_url, is_good_link)
        ## url is _URL_BAD if it has 5 times of 404
        self._404 = {}
        self._404_threshold = 5

    def _load_from_url_index(self, load_bad=False, is_good_link=None):
        print '%schecking url index to get task%s' % (BRO, NOR)
        print 'load_bad:', load_bad
        bad_c = 0
        task_c = 0
        done_c = 0
        self._bad_urls = set()
        for url,state in self._urlindex.RangeIter():
            if is_good_link:
                if not is_good_link(url):
                    #print 'bad url:', url
                    continue
            if state == self._URL_BAD:
                bad_c += 1
                if load_bad:
                    self._bad_urls.add(url)
                continue
            if state == self._URL_TASK:
                task_c += 1
                host = urlparse.urlparse(url).netloc
                if not host or '.' not in host:
                    print 'index has bad url:', url
                    self._urlindex.Put(url, self._URL_DONE)
                    continue
                if host in self._pool:
                    self._pool[host].add(url)
                else:
                    self._pool[host] = set([url])
            if state == self._URL_DONE:
                done_c += 1
        if self._bad_urls and load_bad:
            print 'add bad urls to UrlPool:', len(self._bad_urls)
            for url in self._bad_urls:
                self.add(url, load_bad_url=True, always=True)
        print '%sgot [%s] tasks not been done from url index%s' %(BRO, task_c, NOR)
        self.url_count += task_c
        print '%sgot [%s] url been done from url index%s' %(BRO, done_c, NOR)
        print '%sgot [%s] bad urls from url index%s' %(BRO, bad_c, NOR)

    def get_index_state(self, url):
        try:
            s = self._urlindex.Get(url)
            return s
        except:
            return self._URL_NONE

    def set_url_bad(self, url):
        self._urlindex.Put(url, self._URL_BAD)

    def set_url_done(self, url):
        self._urlindex.Put(url, self._URL_DONE)

    def set_url_404(self, url):
        if url in self._404:
            self._404[url] += 1
            if self._404[url] > self._404_threshold:
                print '%smet bad url:[%s]%s' % (RED, url, NOR)
                self._urlindex.Put(url, self._URL_BAD)
                self._404.pop(url)
        else:
            self._404[url] = 1


    def addmany(self, urls, is_good_link=None, always=False):
        for url in urls:
            if is_good_link:
                if not is_good_link(url):
                    print 'addmany(): bad url:', url
                    continue
            self.add(url, always=always)
        if urls:
            print 'pool url count: ', self.url_count

    def add(self, url, load_bad_url=False, always=False):
        url = url_normalize(url)
        if always:
            state = self._URL_TASK
        else:
            state = self.get_index_state(url)
            if state == self._URL_TASK:
                ## crawling
                return
            if state == self._URL_DONE:
                return
            if not load_bad_url and state == self._URL_BAD:
                return
        host = urlparse.urlparse(url).netloc
        if not host: return
        if host in self._pool:
            self._pool[host].add(url)
        else:
            self._pool[host] = set([url])
        self.url_count += 1
        self._urlindex.Put(url, self._URL_TASK)
        #print 'adding: %s, url_count: %s' % (url, self.url_count,)

    def pop(self, span_threshold=3):
        host = ''
        now = time.time()
        for h in self._pool:
            if h not in self._hosts_pop_recently:
                host = h
                break
            span = now - self._hosts_pop_recently[h]
            if span > span_threshold:
                host = h
                break
        if not host and self.url_count > 300:
            idx = random.randint(0, len(self._pool)-1)
            host = self._pool.keys()[idx]
        if not host:
            print 'UrlPool:: no host got, url_count:', self.url_count
            return ''
        url = self._pool[host].pop()
        if not self._pool[host]:
            del self._pool[host]
        self._hosts_pop_recently[host] = now
        self.url_count -= 1
        return url

    def size(self,):
        c = 0
        for k in self._pool:
            c += len(self._pool[k])
        return c

    def empty(self,):
        #logger.debug('urlpool: %s' % len(self._pool))
        return len(self._pool) == 0


if __name__ == '__main__':
    urls = [
        'http://baidu.com/',
    ]
    up = UrlPool()
    while 1:
        url = up.pop()
        if not url:
            break
        print url

