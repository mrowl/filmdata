import time
import logging
from functools import partial

import filmdata.lib.socks as socks
from filmdata.lib.util import take
import gevent
from gevent import monkey
from gevent.queue import LifoQueue, Queue

import httplib2

log = logging.getLogger(__name__)

monkey.patch_all()

class Struct:
    def __init__(self, **entries): 
        self.__dict__.update(entries)

class Scrape(object):

    def __init__(self, urls, fetch_callback, scrape_callback=None,
                 follow_redirects=True, anon=False, max_clients=10,
                 max_retries=10, delay=0, timeout=3.0):
        self._urls = urls
        self._fetch_callback = self._wrap_callback(fetch_callback)
        self._scrape_callback = scrape_callback
        self._follow_redirects = follow_redirects
        self._max_redirects = 5
        self._max_retries = max_retries
        self._max_clients = max_clients
        self._proxy = {}
        self._delay = delay
        if anon:
            self._proxy = { 'proxy_host' : '127.0.0.1',
                            'proxy_port' : 8118 }
        self._timeout = { 'connect_timeout' : float(timeout) / 2,
                          'request_timeout' : float(timeout) }
        self._headers = {}
        self.add_header('User-agent', 'Mozilla/5.0')

    def add_header(self, key, value):
        self._headers[key] = value

    def run(self):
        log.info('Spooling up "threads" (%d)' % self._max_clients)
        taken = False
        i = 0
        while not taken:
            url_set = take(self._max_clients, self._urls)
            taken = False if len(url_set) == self._max_clients else True
            log.info('Fetching url set %d' % i)
            i += 1

            start_time = time.time()
            self._fetch_urls(url_set)
            end_time = time.time()
            elapsed_time = end_time - start_time

            if not taken and elapsed_time < self._delay:
                time.sleep(1.3 - elapsed_time)
        if self._scrape_callback:
            self._scrape_callback()

    def _fetch_url(self, url, count=0):
        if isinstance(url, tuple):
            key, uri = url
        else:
            uri = url
        kwargs = self._timeout
        kwargs.update(self._proxy)
        if self._proxy:
            proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP,
                                            self._proxy['proxy_host'],
                                            self._proxy['proxy_port'])
            http = httplib2.Http(proxy_info=proxy_info)
        else:
            http = httplib2.Http()
        if self._follow_redirects:
            redirections = self._max_redirects
        else:
            http.follow_redirects = False
            redirections = 0
        resp, content = http.request(uri, headers=self._headers,
                                     redirections=redirections)
        thing = Struct(buffer=content, location=resp.get('location'),
                       effective_url=uri, status=int(resp['status']))
        self._fetch_callback(thing, url=url, retry_count=count)

    def _fetch_urls(self, urls):
        jobs = [ gevent.spawn(self._fetch_url, url) for url in urls ]
        gevent.joinall(jobs)

    def _wrap_callback(self, func):
        def wrapper(resp, url=None, retry_count=None):
            if (retry_count < self._max_retries and resp.status and
                resp.status >= 400):
                log.error("Error (# %d): %s" % (retry_count,
                                                str(resp.status)))
            else:
                func(resp, resp_url=url)
        return wrapper

class ScrapeQueue(object):

    def __init__(self, scrape_callback=None,
                 follow_redirects=True, anon=False, max_clients=10,
                 max_retries=10, delay=0, timeout=3.0, lifo=False):
        self._scrape_callback = scrape_callback
        self._follow_redirects = follow_redirects
        self._max_redirects = 5
        self._max_retries = max_retries
        self._max_clients = max_clients
        self._proxy = {}
        self._delay = delay
        if lifo:
            self.q = LifoQueue()
        else:
            self.q = Queue()
        if anon:
            self._proxy = { 'proxy_host' : '127.0.0.1',
                            'proxy_port' : 8118 }
        self._timeout = { 'connect_timeout' : float(timeout) / 2,
                          'request_timeout' : float(timeout) }
        self._headers = {}
        self.add_header('User-agent', 'Mozilla/5.0')

    def add_header(self, key, value):
        self._headers[key] = value

    def run(self):
        log.info('Spooling up "threads" (%d)' % self._max_clients)
        taken = False
        i = 0
        while not taken:
            url_set = take(self._max_clients, self._urls)
            taken = False if len(url_set) == self._max_clients else True
            log.info('Fetching url set %d' % i)
            i += 1

            start_time = time.time()
            self._fetch_urls(url_set)
            end_time = time.time()
            elapsed_time = end_time - start_time

            if not taken and elapsed_time < self._delay:
                time.sleep(1.3 - elapsed_time)
        if self._scrape_callback:
            self._scrape_callback()

    def _fetch_url(self, url, count=0):
        if isinstance(url, tuple):
            key, uri = url
        else:
            uri = url
        kwargs = self._timeout
        kwargs.update(self._proxy)
        if self._proxy:
            proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP,
                                            self._proxy['proxy_host'],
                                            self._proxy['proxy_port'])
            http = httplib2.Http(proxy_info=proxy_info)
        else:
            http = httplib2.Http()
        if self._follow_redirects:
            redirections = self._max_redirects
        else:
            http.follow_redirects = False
            redirections = 0
        resp, content = http.request(uri, headers=self._headers,
                                     redirections=redirections)
        thing = Struct(buffer=content, location=resp.get('location'),
                       effective_url=uri, status=int(resp['status']))
        return thing

    def _fetch_urls(self, urls):
        jobs = [ gevent.spawn(self._fetch_url, url) for url in urls ]
        gevent.joinall(jobs)

    def _wrap_callback(self, func):
        def wrapper(resp, url=None, retry_count=None):
            if (retry_count < self._max_retries and resp.status and
                resp.status >= 400):
                log.error("Error (# %d): %s" % (retry_count,
                                                str(resp.status)))
            else:
                func(resp, resp_url=url)
        return wrapper
