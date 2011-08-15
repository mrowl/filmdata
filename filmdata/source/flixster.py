import time
import logging
import urllib
import json
from itertools import imap, islice, ifilter
from decimal import Decimal
from operator import itemgetter
import gevent

from filmdata.lib.scrape import Scrape, ScrapeQueue

import filmdata
from filmdata.lib.util import dson, class_property
from filmdata import config

log = logging.getLogger(__name__)

class FlixsterScrape(ScrapeQueue):

    def __init__(self):
        ScrapeQueue.__init__(self)
        self._dispatch = {
            'info' : self.handler_info,
            'review' : self.handler_review,
            'scan' : self.handler_scan,
        }
        self._max_clients = 8
        self._delay = 1.2
        self._max_requests = 3000
        self._request_count = 0
        self._workers = []
        self._scan_args = { 'apikey' : Fetch._api_key,
                            'page_limit' : 50 }

    def run(self):
        self.q.put(None)
        for url, query in self.get_scan_urls():
            self.q.put({
                'type' : 'scan',
                'url' : url,
                'kwargs' : { 'query' : query },
            })

        for id, url in self.get_info_urls():
            self.q.put({
                'type' : 'info',
                'url' : url,
                'kwargs' : { 'id' : id },
            })

        for url in self.get_list_urls():
            self.q.put({
                'type' : 'scan',
                'url' : url,
            })

        self._workers = [ gevent.spawn(self.worker) for i in
                          range(self._max_clients) ]
        log.info('Launched %d workers.' % self._max_clients)
        gevent.joinall(self._workers)
        self.finish()

    def finish(self):
        Fetch._dump(type='ids')
        Fetch._dump(type='info')
        Fetch._dump(type='reviews')

    def worker(self):
        while True:
            if self._request_count > self._max_requests:
                return
            item = self.q.get()
            if not item:
                self.q.put(None)
                return
            start_time = time.time()
            self._dispatch[item['type']](item['url'],
                                         **item.get('kwargs', {}))
            end_time = time.time()
            elapsed_time = end_time - start_time
            if elapsed_time < self._delay:
                time.sleep(self._delay - elapsed_time)
            self._request_count += 1

    def handler(func):
        def wrapper(self, url, **kwargs):
            log.debug('Fetching url: %s' % url)
            resp = self._fetch_url(url)
            if resp.status and resp.status >= 400:
                log.error("Error (# 0): %s" % str(resp.status))
            else:
                func(self, resp, **kwargs)
        return wrapper

    @handler
    def handler_info(self, resp, id):
        title = json.loads(resp.buffer.strip())
        # make sure it's legit (need the title for later
        # to test if fetched
        if title.get('id') and title.get('title'):
            title['id'] = int(title['id'])
            log.info('Added flixster title %d' % title['id'])
            filmdata.sink.store_source_fetch('flixster_title', title)
        return True

    @handler
    def handler_review(self, resp, id):
        content = json.loads(resp.buffer.strip())
        # make sure it's legit (need the title for later
        # to test if fetched
        if content['links'].get('next'):
            args = urllib.urlencode(self._scan_args)
            self.q.put({
                'type' : 'review',
                'url' : '&'.join((list['links']['next'], args)),
                'kwargs' : { 'id' : id },
            })

        if content.get('reviews'):
            item = { 'id' : id, 'reviews' : content['reviews'] }
            filmdata.sink.store_source_fetch('flixster_review', item)
            log.info('Added flixster review %d' % id)
        return True

    @handler
    def handler_scan(self, resp, query=None):
        if isinstance(resp, basestring):
            buffer = resp
        else:
            buffer = resp.buffer
        content = json.loads(buffer.strip())

        if content['links'].get('next'):
            args = urllib.urlencode(self._scan_args)
            self.q.put({
                'type' : 'scan',
                'url' : '&'.join((content['links']['next'], args)),
            })

        list = content.get('movies', [])
        for id in [ int(m['id']) for m in list if
                    int(m['id']) not in Fetch.known_ids ]:
            filmdata.sink.store_source_fetch('flixster_title', { 'id' : id })
            Fetch.known_ids.add(id)
            log.info('Added flixster id %d' % id)
            self.q.put({
                'type' : 'info',
                'url' : self.get_info_url(id)[1], 
                'kwargs' : { 'id' : id },
            })
            self.q.put({
                'type' : 'review',
                'url' : self.get_review_url(id)[1],
                'kwargs' : { 'id' : id },
            })
        if query:
            filmdata.sink.store_source_fetch('flixster_title_search_log', query)

    def get_review_url(self, id):
        args = urllib.urlencode({ 'apikey' : Fetch._api_key,
                                  'page_limit' : 50,
                                  'page' : 1,
                                  'review_type' : 'top_critic' })
        return (id,
                '?'.join((config.flixster.title_info_url + str(id) + '/reviews.json',
                          args)))

    def get_scan_url(self, query, page=1):
        args = self._scan_args.copy()
        args.update({
            'q' : query.lower().encode('utf-8'),
            'page' : page,
        })
        return '?'.join((config.flixster.title_search_url,
                         urllib.urlencode(args)))

    def get_scan_urls(self): 
        searched_primary_ids = set(map(itemgetter('id'),
                                       Fetch._get_logged_searches()))
        unsearched_test = lambda t: t['id'] not in searched_primary_ids

        title_iter = ifilter(unsearched_test, filmdata.sink.get_titles())
        for title in title_iter:
            yield [self.get_scan_url(title['name']), { 'id' : title['id'],
                                                       'name' : title['name']}]
    
    def get_info_url(self, id):
        args = urllib.urlencode({ 'apikey' : Fetch._api_key, })
        return (id,
                '?'.join((config.flixster.title_info_url + str(id) + '.json',
                          args)))

    def get_info_urls(self):
        unfetched_title_ids = map(itemgetter('id'),
                                  filter(lambda t: t.get('title') is None,
                                         Fetch._get_fetched_info(type='title')))
        if len(unfetched_title_ids) == 0:
            log.info('no ids left for which to fetch titles')
            Fetch._dump_titles(type='info')
            return

        unfetched_title_ids.sort()
        ids_to_fetch = unfetched_title_ids

        log.info('Starting at id %s' % str(ids_to_fetch[0]))
        return imap(self.get_info_url, iter(ids_to_fetch))

    def get_list_urls(self):
        args = urllib.urlencode({ 'apikey' : Fetch._api_key,
                                  'page_limit' : 50 })
        paths = ( config.flixster.title_theaters_url,
                  config.flixster.title_opening_url,
                  config.flixster.title_released_url,
                  config.flixster.title_releasing_url, )
        url_maker = lambda p: '?'.join((p, args))
        return map(url_maker, paths)

class Fetch:

    name = 'flixster'

    _rating_factor = int(config.core.max_rating) / 5
    _api_key = config.flixster.key
    _max_threads = 8
    _max_requests = 3200

    @class_property
    @classmethod
    def known_ids(cls):
        if not hasattr(cls, '_known_ids'):
            cls._known_ids = set(cls._get_known_ids())
        return cls._known_ids

    @classmethod
    def fetch_data(cls, pull_ids=False):
        scraper = FlixsterScrape()
        scraper.run()
        #cls.fetch_new_ids()
        #cls.fetch_info()

    @classmethod
    def fetch_ids(cls, title_types=None, id_types=None):
        arg_maker = lambda title: {
            'apikey' : cls._api_key,
            'q' : title['name'].lower().encode('utf-8'),
            'page_limit' : 50,
            'page' : 1,
        }
        unsearched_test = lambda t: t['id'] not in searched_primary_ids
        url_maker = lambda args: '?'.join((config.flixster.title_search_url,
                                           urllib.urlencode(args)))

        def mark_searched(title):
            filmdata.sink.store_source_fetch('flixster_title_search_log',
                                             { 'id' : title['id'],
                                               'name' : title['name'], })
            return title

        searched_primary_ids = set(map(itemgetter('id'),
                                       cls._get_logged_searches()))
        titles = filmdata.sink.get_source_titles(config.core.primary_title_source)
        title_iter = imap(mark_searched, ifilter(unsearched_test, titles))
        url_source = islice(imap(url_maker, imap(arg_maker, title_iter)),
                            0, cls._max_requests)

        scraper = Scrape(url_source, cls._add_new_ids,
                         max_clients=8, anon=False, delay=1.3)
        scraper.run()
        cls._dump(type='ids')

    @classmethod
    def fetch_new_ids(cls):
        args = urllib.urlencode({ 'apikey' : cls._api_key,
                                  'page_limit' : 50 })
        paths = ( config.flixster.title_theaters_url,
                  config.flixster.title_opening_url,
                  config.flixster.title_released_url,
                  config.flixster.title_releasing_url, )
        url_maker = lambda p: '?'.join((p, args))
        for url in map(url_maker, paths):
            url_next = url
            while url_next:
                log.info('Fetching/adding Flixster list: %s' % url_next)
                content = urllib.urlopen(url_next).read().strip()
                if content:
                    list = cls._add_new_ids(content, url)
                    if list['links'].get('next'):
                        url_next = '&'.join((list['links']['next'], args))
                    else:
                        url_next = None
                else:
                    url_next = None

    @classmethod
    def fetch_info(cls):
        def handler(resp, resp_url):
            title = json.loads(resp.buffer.strip())
            # make sure it's legit (need the title for later
            # to test if fetched
            if title.get('id') and title.get('title'):
                title['id'] = int(title['id'])
                filmdata.sink.store_source_fetch('flixster_title', title)
            return True

        unfetched_title_ids = map(itemgetter('id'),
                                  filter(lambda t: t.get('title') is None,
                                         cls._get_fetched_info(type='title')))
        if len(unfetched_title_ids) == 0:
            log.info('no ids left for which to fetch titles')
            cls._dump_titles(type='info')
            return

        unfetched_title_ids.sort()
        if len(unfetched_title_ids) > cls._max_requests:
            ids_to_fetch = unfetched_title_ids[:cls._max_requests]
        else:
            ids_to_fetch = unfetched_title_ids

        args = urllib.urlencode({ 'apikey' : cls._api_key, })
        url_maker = lambda id: '?'.join((config.flixster.title_info_url + str(id) + '.json',
                                         args))

        log.info('Starting at id %s' % str(ids_to_fetch[0]))
        url_source = islice(imap(url_maker, iter(ids_to_fetch)),
                            0, cls._max_requests)
        scraper = Scrape(url_source, handler,
                         max_clients=8, anon=True, delay=1.3)
        scraper.run()
        cls._dump(type='info')

    #@classmethod
    #def fetch_reviews(cls):
        #def handler(resp, resp_url):

    @classmethod
    def _add_new_ids(cls, resp, resp_url=None):
        if isinstance(resp, basestring):
            buffer = resp
        else:
            buffer = resp.buffer
        list = json.loads(buffer.strip())
        for id in [ int(m['id']) for m in list['movies'] if
                    int(m['id']) not in cls.known_ids ]:
            log.debug('Added flixster id %d' % id)
            filmdata.sink.store_source_fetch('flixster_title', { 'id' : id })
        return list

    @classmethod
    def _dump(cls, type='ids'):
        if type == 'ids':
            log.info('dumping ids to %s' % config.flixster.titles_path)
            json.dump(list(cls.known_ids),
                      open(config.flixster.title_ids_path, 'w'))
        elif type == 'reviews':
            log.info('dumping reviews to %s' %
                     config.flixster.title_reviews_path)
            dson.dump(dict([ (i['id'], i) for i in
                             cls._get_fetched_info('reviews') ]),
                      config.flixster.titles_path)
        else:
            log.info('dumping titles to %s' % config.flixster.titles_path)
            dson.dump(dict([ (i['id'], i) for i in
                             cls._get_fetched_info('title') ]),
                      config.flixster.titles_path)
        log.info('dump complete')

    @classmethod
    def _get_fetched_info(cls, type='title'):
        return filmdata.sink.get_source_fetch('flixster_%s' % type)

    @classmethod
    def _get_known_ids(cls, type='title'):
        return map(itemgetter('id'),
                   filmdata.sink.get_source_fetch('flixster_%s' % type,
                                                  ids_only=True))

    @classmethod
    def _get_logged_searches(cls, type='title'):
        return filmdata.sink.get_source_fetch('flixster_%s_search_log' % type)

class Produce:

    name = 'flixster'

    _source_max_rating = 100
    _global_max_rating = int(config.core.max_rating)
    _rating_factor = Decimal(_global_max_rating) / _source_max_rating

    @classmethod
    def produce_titles(cls, types):
        
        flix_titles = Fetch._get_fetched_info(type='title')
        for flix_title in flix_titles:
            if not flix_title.get('title') or not flix_title.get('year'):
                continue
            id = int(flix_title['id'])
            yield {
                'id' : id,
                'alternate' : cls._get_alternate(flix_title.get('alternate_ids')),
                'name' : flix_title.get('title'),
                'year' : flix_title.get('year'),
                'runtime' : flix_title.get('runtime'),
                'synopsis' : flix_title.get('synopsis'),
                'href' : flix_title['links'].get('alternate'),
                'genre' : flix_title.get('genres'),
                'mpaa' : {
                    'rating' : flix_title.get('mpaa_rating'),
                },
                'consensus' : flix_title.get('critics_consensus'),
                'review' : cls._get_reviews(id),
                'ratings' : cls._get_ratings(flix_title.get('ratings')),
                'cast' : cls._get_cast(flix_title.get('abridged_cast')),
                'director' : cls._get_directors(flix_title.get('abridged_directors')),
                'art' : cls._get_art(flix_title.get('posters')),
            }

    @classmethod
    def _get_reviews(cls, id):
        title = filmdata.sink.get_source_fetch_by_id('flixster_review', id)
        if not title:
            return None
        return title.get('reviews')

    @classmethod
    def _get_alternate(cls, alternate_ids):
        if not alternate_ids:
            return None
        if alternate_ids.get('imdb') and alternate_ids['imdb'].isdigit():
            alternate_ids['imdb'] = int(alternate_ids['imdb'])
        return alternate_ids

    @classmethod
    def _get_art(cls, posters):
        if not posters:
            return None
        art = {}
        if posters.get('det'):
            art['large'] = posters.get('det')
        if posters.get('profile'):
            art['small'] = posters.get('profile')
        return art

    @classmethod
    def _get_directors(cls, abridged_directors):
        if not abridged_directors:
            return None
        directors = []
        for i, member in enumerate(abridged_directors):
            directors.append({
                'name' : member['name'],
                'billing' : i+1,
            })
        return directors

    @classmethod
    def _get_cast(cls, abridged_cast):
        if not abridged_cast:
            return None
        cast = []

        for i, member in enumerate(abridged_cast):
            if member.get('characters'):
                character = member.get('characters')[0]
            else:
                character = None
            cast.append({
                'name' : member['name'],
                'character' : character,
                'billing' : i+1,
            })
        return cast
    @classmethod
    def _get_ratings(cls, ratings):
        if not ratings:
            return None
        result = {}
        for source, type in (('rt', 'critics'), ('flixster', 'audience')):
            score = ratings.get('%s_score' % type)
            label = ratings.get('%s_rating' % type)
            if score and score > 0:
                mean = cls._rating_factor * Decimal(score)
                result[source] = { 'mean' :  mean}
                if label:
                    result[source]['label'] = label
        return result
