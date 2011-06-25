import os
import logging
import urllib
import json
import itertools
from collections import defaultdict

from twisted.web.client import reactor, defer, getPage

from filmdata import config
import filmdata

log = logging.getLogger(__name__)

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(itertools.islice(iterable, n))

class Fetch:

    name = 'flixster'

    _rating_factor = int(config.core.max_rating) / 5
    _api_key = config.flixster.key
    _max_threads = 8
    _threads_finished = 0
    _max_requests = 4000
    
    @classmethod
    def fetch_data(cls, pull_ids=False):
        cls.fetch_ids()

    @classmethod
    def fetch_ids(cls):
        arg_maker = lambda title: {
            'apikey' : cls._api_key,
            'q' : title['name'].lower().encode('utf-8'),
            'page_limit' : 50,
            'page' : 1,
        }

        url_maker = lambda args: '?'.join((config.flixster.title_search_url,
                                           urllib.urlencode(args)))
        ids = []
        
        def store_ids(results, last):
            reactor.stop()
            prev_set = json.load(open(config.flixster.ids_path))
            ids.extend(prev_set)
            uniques = list(set(ids))
            #flatter = [ id for sub_k, sub_v in id_lists for k, id in sub_v ]
            
            json.dump(uniques, open(config.flixster.ids_path, 'w'))
            print 'Dumped %d ids' % len(uniques)
            json.dump(last, open(config.flixster.last_query_path, 'w'))
            print 'ended at %s' % str(title_set[-1]['key'])

        def fetch_set(urls, last=None):

            def handler(body):
                print 'in handler'
                resp = json.loads(body.strip())
                for movie in resp['movies']:
                    ids.append(int(movie['id']))
                return True

            deferreds = []
            for url in urls:
                deferreds.append(getPage(url).addCallback(handler))
            dl = defer.DeferredList(deferreds)
            if last is not None:
                dl.addCallback(store_ids, last)
            return dl


        titles = filmdata.sink.get_titles('netflix')
        if os.path.exists(config.flixster.last_query_path):
            last_query = json.load(open(config.flixster.last_query_path))
            print 'starting at %s' % str(last_query)
            title_iter = itertools.dropwhile(lambda t: t['key'] != last_query, titles)
        else:
            title_iter = titles
        i = 0
        while i < cls._max_requests:
            delay = 1.3*i/cls._max_threads
            i += cls._max_threads
            title_set = take(cls._max_threads, title_iter)
            url_set = map(url_maker, map(arg_maker, title_set))
            last = title_set[-1]['key'] if i >= cls._max_requests else None
            reactor.callLater(delay, fetch_set, url_set, last=last)
        reactor.run()
