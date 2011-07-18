from functools import partial
import logging
from tornado import httputil
from tornado.httpclient import AsyncHTTPClient
import tornado.ioloop

log = logging.getLogger(__name__)

class Scrape(object):

    def __init__(self, urls, fetch_callback, scrape_callback=None,
                 follow_redirects=True, anon=False, max_clients=10,
                 max_retries=10, timeout=3.0):
        self._urls = urls
        self._fetch_callback = self._wrap_callback(fetch_callback)
        self._scrape_callback = scrape_callback
        self._follow_redirects = follow_redirects
        self._max_retries = max_retries
        self._max_clients = max_clients
        self._proxy = {}
        if anon:
            self._proxy = { 'proxy_host' : '127.0.0.1',
                            'proxy_port' : 8118 }
        self._timeout = { 'connect_timeout' : float(timeout) / 2,
                          'request_timeout' : float(timeout) }
        self._headers = httputil.HTTPHeaders()
        self._ioloop = tornado.ioloop.IOLoop.instance()
        self._create_client()
        self.add_header('User-agent', 'Mozilla/5.0')

    def add_header(self, key, value):
        self._headers.add(key, value)

    def run(self):
        print 'Spooling up "threads" (%d)' % self._max_clients
        started = False
        for i, url in enumerate(self._urls):
            self._fetch(url)
            if i > self._max_clients:
                started = True
                self._ioloop.start()
        not started and self._ioloop.start()
        self._scrape_callback()

    def _fetch(self, url, count=0):
        if isinstance(url, tuple):
            key, uri = url
        else:
            uri = url
        kwargs = self._timeout
        kwargs.update(self._proxy)
        callback = partial(self._fetch_callback, url=url, retry_count=count)
        self._client.fetch(str(uri), callback,
                           follow_redirects=self._follow_redirects,
                           headers=self._headers,
                           **kwargs)

    def _wrap_callback(self, func):
        def wrapper(resp, url=None, retry_count=None):
            if (retry_count < self._max_retries and resp.error and
                getattr(resp.error, 'code', 0) >= 400):
                if retry_count > 1:
                    log.error("Error (# %d): %s" % (retry_count,
                                                    str(resp.error)))
                self._fetch(url, count=retry_count+1)
            else:
                func(resp, resp_url=url)
            if not self._client._requests:
                self._ioloop.stop()
        return wrapper

    def _create_client(self):
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient",
                                  max_simultaneous_connections=self._max_clients)
        self._client = AsyncHTTPClient(self._ioloop,
                                       max_clients=self._max_clients)
