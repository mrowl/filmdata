import logging
import urllib
import json
from itertools import imap, islice, ifilter
from decimal import Decimal
from operator import itemgetter

from filmdata.lib.scrape import Scrape

import filmdata
from filmdata.lib.util import dson
from filmdata import config

log = logging.getLogger(__name__)

class Fetch:

    name = 'flixster'

    _rating_factor = int(config.core.max_rating) / 5
    _api_key = config.flixster.key
    _max_threads = 8
    _max_requests = 3200
    
    @classmethod
    def fetch_data(cls, pull_ids=False):
        cls.fetch_info()

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

        def handler(resp, resp_url=None):
            resp = json.loads(resp.buffer.strip())
            for id in [ int(m['id']) for m in resp['movies'] if
                        int(m['id']) not in known_ids ]:
                filmdata.sink.store_source_fetch('flixster_title', { 'id' : id })
            return True

        known_ids = set(cls._get_known_ids())

        searched_primary_ids = set(map(itemgetter('id'),
                                       cls._get_logged_searches()))
        titles = filmdata.sink.get_source_titles(config.core.primary_title_source)
        title_iter = imap(mark_searched, ifilter(unsearched_test, titles))
        url_source = islice(imap(url_maker, imap(arg_maker, title_iter)),
                            0, cls._max_requests)

        scraper = Scrape(url_source, handler,
                         max_clients=8, anon=True, delay=1.3)
        scraper.run()
        cls._dump(type='ids')

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

    @classmethod
    def _dump(cls, type='ids'):
        if type == 'ids':
            log.info('dumping ids to %s' % config.flixster.titles_path)
            json.dump(cls._get_known_ids(),
                      open(config.flixster.title_ids_path, 'w'))
        else:
            log.info('dumping titles to %s' % config.flixster.titles_path)
            dson.dump(dict([ (i['id'], i) for i in
                             cls._get_fetched_info() ]),
                      config.flixster.titles_path)
        log.info('dump complete')

    @classmethod
    def _get_fetched_info(cls, type='title'):
        return filmdata.sink.get_source_fetch('flixster_title')

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
            yield {
                'id' : int(flix_title['id']),
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
                'ratings' : cls._get_ratings(flix_title.get('ratings')),
                'cast' : cls._get_cast(flix_title.get('abridged_cast')),
                'director' : cls._get_directors(flix_title.get('abridged_directors')),
                'art' : cls._get_art(flix_title.get('posters')),
            }

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
