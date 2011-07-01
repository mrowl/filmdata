import os
from decimal import Decimal
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
    _max_requests = 4000
    
    @classmethod
    def fetch_data(cls, pull_ids=False):
        #cls.fetch_ids()
        #if pull_ids:
        cls.fetch_info()

    @classmethod
    def fetch_info(cls):
        load_tmp_titles = lambda: map(json.loads, [ l.strip() for l in
                                                    open(config.flixster.titles_tmp_path) ])
        def finish(results, last):
            reactor.stop()
            fetched_titles = load_tmp_titles()
            if os.path.exists(config.flixster.titles_path):
                titles = json.load(open(config.flixster.titles_path))
            else:
                titles = {}
            for title in fetched_titles:
                titles[title['id']] = title
            json.dump(titles, open(config.flixster.titles_path, 'w'))
            os.remove(config.flixster.titles_tmp_path)
            print 'ended at %s' % str(last)

        def fetch_set(urls, last=None):

            def handler(body):
                print 'in handler'
                title = json.loads(body.strip())
                if 'id' in title:
                    title_jsoned = json.dumps(title)
                    f = open(config.flixster.titles_tmp_path, 'a')
                    f.write(title_jsoned + '\n')
                    f.close()
                return True

            deferreds = []
            for url in urls:
                deferreds.append(getPage(url).addCallback(handler))
            dl = defer.DeferredList(deferreds)
            if last is not None:
                dl.addCallback(finish, last)
            return dl

        fetched_titles = []
        fetched_title_ids = []
        if os.path.exists(config.flixster.titles_tmp_path):
            fetched_titles = load_tmp_titles()
            fetched_title_ids = [ int(title['id']) for title in fetched_titles ]

        if os.path.exists(config.flixster.titles_path):
            fetched_title_ids += [ int(k) for k in 
                json.load(open(config.flixster.titles_path)).keys() ]

        available_ids = json.load(open(config.flixster.ids_path))
        unfetched_title_ids = list(set(available_ids) - set(fetched_title_ids))
        unfetched_title_ids.sort()

        if len(unfetched_title_ids) > cls._max_requests:
            ids_to_fetch = unfetched_title_ids[:cls._max_requests]
        elif len(unfetched_title_ids) == 0:
            return
        else:
            ids_to_fetch = unfetched_title_ids

        args = urllib.urlencode({ 'apikey' : cls._api_key, })
        url_maker = lambda id: '?'.join((config.flixster.title_info_url + str(id) + '.json',
                                         args))
        i = 0
        print 'starting at %s' % str(ids_to_fetch[0])
        id_iter = iter(ids_to_fetch)
        while i < cls._max_requests:
            delay = 1.3*i/cls._max_threads
            i += cls._max_threads
            id_set = take(cls._max_threads, id_iter)
            url_set = map(url_maker, id_set)
            last = id_set[-1] if i >= cls._max_requests else None
            reactor.callLater(delay, fetch_set, url_set, last=last)
        reactor.run()

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
            uniques.sort()
            #flatter = [ id for sub_k, sub_v in id_lists for k, id in sub_v ]
            
            json.dump(uniques, open(config.flixster.ids_path, 'w'))
            print 'Dumped %d ids' % len(uniques)
            json.dump(last, open(config.flixster.last_id_query_path, 'w'))
            print 'ended at %s' % str(last)

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


        titles = filmdata.sink.get_source_titles('netflix')
        if os.path.exists(config.flixster.last_id_query_path):
            last_query = json.load(open(config.flixster.last_id_query_path))
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

class Produce:

    name = 'flixster'

    _source_max_rating = 100
    _global_max_rating = int(config.core.max_rating)
    _rating_factor = Decimal(_global_max_rating) / _source_max_rating

    @classmethod
    def produce_titles(cls, types):
        flix_titles = json.load(open(config.flixster.titles_path))
        for flix_id, flix_title in flix_titles.iteritems():
            if not flix_title.get('title'):
                continue
            if not flix_title.get('year'):
                continue
            yield {
                'key' : flix_title['id'],
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
            }

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
