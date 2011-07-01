import logging
import re
import time
import itertools
from filmdata.match import fuzz
from collections import OrderedDict
from functools import partial
from decimal import Decimal
from operator import itemgetter

import pymongo as pmongo
import asyncmongo as amongo
import tornado.ioloop

from filmdata import config

log = logging.getLogger(__name__)

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(itertools.islice(iterable, n))

def take_slices(size, iterable):
    while True:
        slice = take(size, iterable)
        if len(slice) > 0:
            yield slice
        if len(slice) < size:
            break

class MongoSink:

    @property
    def am(self):
        if self._am is None:
            self._am = amongo.Client('filmdata', **self._args)
        return self._am

    @property
    def m(self):
        if self._m is None:
            self._m = pmongo.Connection(self._args['host'], 
                                         self._args['port'])[self._args['dbname']]
        return self._m

    @property
    def io(self):
        if self._io is None:
            self._io = tornado.ioloop.IOLoop.instance()
        return self._io


    def __init__(self):
        self._args = {
            'port' : int(config.mongo.port),
            'maxcached' : int(config.mongo.maxcached),
            'maxconnections' : int(config.mongo.maxconnections),
            'host' : config.mongo.host,
            'dbname' : config.mongo.dbname,
        }
        self._m = None
        self._am = None
        self._io = None
        self._io_free = True
        self._start = time.time()

    def ensure_indexes(self):
        base_key_index = {
            'key.imdb' : pmongo.ASCENDING,
            'key.netflix' : pmongo.ASCENDING,
            'key.flixster' : pmongo.ASCENDING,
        }
        self.m.title.ensure_index(base_key_index.items())
        self.m.person.ensure_index(base_key_index.items())

    def consume_merged_titles(self, producer):
        self.ensure_indexes()
        for title in itertools.imap(self._remap_id, producer):
            if not title.get('_id'):
                title['_id'] = self._get_seq_id('title')
                self.m.title.insert(title)
            else:
                self.m.title.update({'_id' : title['_id'] }, title,
                                    upsert=False, multi=False)

    def consume_merged_persons(self, producer):
        self.ensure_indexes()
        for person in itertools.imap(self._remap_id, producer):
            self.m.person.update(self._get_keys_spec(person['key']), person,
                                 upsert=True, multi=False)

    # TODO: only produce source titles which aren't matched yet
    def get_titles_for_matching(self, source):
        collection = '%s_title' % source
        re_title_fixer = re.compile(':[^:]*? (edition|cut)$')
        for title in self.m[collection].find():
            matcher = {
                'key' : title['_id'],
                'name' : re_title_fixer.sub('', title['name'].lower()),
                'year' : title['year'],
                'votes' : 'rating' in title and title['rating'].get('count') or 0,
            }
            if title.get('aka'):
                matcher['aka'] = frozenset([ a['name'].lower() for a in
                                             title['aka'] ])
                matcher['aka_year'] = frozenset([ int(a['year']) for a in
                                                   title['aka'] if a['year'] ])
            else:
                akas = matcher['name'].split(' / ')
                if len(akas) > 1:
                    matcher['aka'] = frozenset(akas)
            if title.get('director'):
                matcher['director'] = frozenset([ fuzz(d['name']) for d in
                                                  title['director'] ])
            if title.get('cast'):
                cast = [ c for c in title['cast'] if
                         c['billing'] and c['billing'] <= 8 ]
                cast.sort(key=itemgetter('billing'))
                matcher['cast'] = frozenset([ fuzz(c['name']) for c in cast ])
            yield matcher

    def consume_source_titles(self, producer, source_name):
        start = time.time()
        collection = '%s_title' % source_name
        for title_in in itertools.imap(self._clean_document, producer):
            title = dict([ (k, v) for k,v in title_in.items() if
                           k not in ('key', 'noinsert') ])
            has_title = self.m[collection].find_one(
                { '_id' : title_in['key'] })
            if (not has_title and not 
                ('noinsert' in title_in and title_in['noinsert'])):
                title['_id'] = title_in['key']
                self.m[collection].insert(title)
            else:
                self.m[collection].update(
                    { '_id' : title_in['key'] },
                    { '$set' : title },
                    upsert=False, multi=False)

        log.info('Finished importing titles (%ss)' % str(time.time() - start))

    def consume_roles(self, producer, source_name=None):
        start = time.time()
        collection_person = 'person' if source_name is None else 'person_%s' % source_name
        collection_title = 'title' if source_name is None else 'title_%s' % source_name
        self.m[collection_person].ensure_index('key', unique=True)
        self.m[collection_title].ensure_index('key', unique=True)

        prev_person_key = None
        for role in itertools.imap(self._clean_document, producer):
            title = self.m[collection_title].find_one(
                { 'key' : role['title']['key'] })
            if not title is None:
                if role['person']['key'] != prev_person_key:
                    self.m[collection_person].insert(role['person'])
                    prev_person_key = role['person']['key']

                role['role']['person'] = role['person']['key']
                self.m[collection_title].update(
                    { 'key' : role['title']['key'] },
                    { '$addToSet' : { 'role' : role['role'] } },
                    upsert=False, multi=False)
            else:
                log.debug('Role title ' + role['title']['ident'] + ' not found')
        log.info('Finished importing titles (%ss)' % str(time.time() - start))

    def get_titles(self):
        return itertools.imap(self._remap_id, self.m.title.find())
    
    def get_title_persons(self):
        for title in self.m.title.find():
            persons = []
            for producers in title['production'].values():
                persons.extend(producers)
            persons.extend(title.cast)
            yield persons

    def get_source_titles(self, source_name, min_votes=0):
        collection = '%s_title' % source_name
        return itertools.imap(partial(self._remap_id, key='key'),
            self.m[collection].find(sort=[('_id', pmongo.ASCENDING)]))

    def get_source_title_by_key(self, name, key):
        collection = '%s_title' % name
        doc = self.m[collection].find_one({'_id' : key })
        return self._remap_id(doc, key='key') if doc else None

    def _get_seq_id(self, collection):
        return self.m.seq.find_and_modify({ '_id' : collection },
                                          { '$inc' : { 'seq' : 1 } },
                                          upsert=True,
                                          new=True)['seq']

    def _remap_id(self, thing, key='id'):
        if '_id' in thing:
            thing[key] = thing['_id']
            del thing['_id']
        elif key in thing:
            thing['_id'] = thing[key]
            del thing[key]
        return thing

    def _get_keys_spec(self, keys_dict, root_key='key'):
        dotted_keys = self._deep_object_and(keys_dict, root_key)
        return dict([ [k, { '$in' : ( v, None ) }] for
                      k, v in dotted_keys.items() ])

    def _deep_object_and(self, match, root_key=''):
        dot_match = {}
        for k, v in match.items():
            dot_key = '.'.join((root_key, k)).lstrip('.')
            if isinstance(v, dict):
                sub_match = self._deep_object_and(v, dot_key)
                for sk, sv in sub_match.items():
                    dot_match[sk] = sv
            else:
                dot_match[dot_key] = v
        return dot_match

    def _clean_document(self, doc):
        if isinstance(doc, dict):
            for k in doc.keys():
                if isinstance(k, int):
                    doc[str(k)] = self._clean_document(doc[k])
                    del doc[k]
                else:
                    doc[k] = self._clean_document(doc[k])
        elif isinstance(doc, tuple):
            doc = [ v for v in doc ]
            doc = self._clean_document(doc)
        elif isinstance(doc, list):
            for i in range(len(doc)):
                doc[i] = self._clean_document(doc[i])
        elif isinstance(doc, Decimal):
            doc = float(doc)
        return doc

    def __get_title_id(self, title):
        m = md5()
        m.update('_'.join(map(':'.join, title.iteritems())).encode('utf-8'))
        return m.hexdigest()

    def __update(self, spec, doc=None):
        if not doc:
            doc = spec
        self.__t.update(spec, doc, False, False, self.__safe)

    def __upsert(self, spec, doc=None):
        if not doc:
            doc = spec
        self.__t.update(spec, doc, True, False, self.__safe)

if __name__ == "__main__":
    pass
