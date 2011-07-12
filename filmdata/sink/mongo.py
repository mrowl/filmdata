import logging
import re
import time
from datetime import datetime
from itertools import islice, imap
from functools import partial
from decimal import Decimal
from operator import itemgetter

import pymongo as pmongo
import asyncmongo as amongo
import tornado.ioloop

from filmdata import config
from filmdata.match import fuzz, match_iter

log = logging.getLogger(__name__)

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))

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
        self._role_groups = config.role_groups
        self.ensure_indexes()

    def ensure_indexes(self):
        base_thing_index = {
            'alternate.imdb' : pmongo.ASCENDING,
            'alternate.netflix' : pmongo.ASCENDING,
            'alternate.flixster' : pmongo.ASCENDING,
        }
        self.m.title.ensure_index(base_thing_index.items())
        self.m.person.ensure_index(base_thing_index.items())
        base_match_index = {
            'imdb' : pmongo.ASCENDING,
            'netflix' : pmongo.ASCENDING,
            'flixster' : pmongo.ASCENDING,
        }
        self.m.title_match.ensure_index(base_match_index.items())
        for group in self._role_groups:
            self.m.title.ensure_index([('.'.join((group, 'person_id')),
                                        pmongo.ASCENDING)])

    def consume_merged_titles(self, producer):
        self._consume_merged_things(producer, 'title')

    def consume_merged_persons(self, producer):
        self._consume_merged_things(producer, 'person')

    def _consume_merged_things(self, producer, thing_type):
        for thing in imap(self._clean, producer):
            if not thing.get('_id'):
                raise Exception('This %s has no id' % thing_type)
            self.m[thing_type].update({'_id' : thing['_id'] }, thing,
                                      upsert=True, multi=False)
            self.update_match_status(thing_type, thing['_id'], 'merged')

    def consume_metric(self, name, producer):
        for id, row in producer:
            if name in ('title', 'person'):
                self.m[name].update(
                    { '_id' : id },
                    { '$set' : self._dict_to_dot(row, root_key='rating') },
                    upsert=False, multi=False)

    def store_source_data(self, source, data, id=None, suffix=None):
        collection = '%s_data' % source
        if suffix:
            collection = '_'.join((collection, suffix))
        if id:
            data.update({ '_id' : id })
            data.update(self._get_timestamps())
            self.m[collection].update({ '_id' : id }, data, upsert=True,
                                      multi=False)
        else:
          self.m[collection].insert(data)

    def get_source_data(self, source, suffix=None):
        collection = '%s_data' % source
        if suffix:
            collection = '_'.join((collection, suffix))
        return imap(self._clean, self.m[collection].find())
    
    def remove_source_data(self, source, suffix=None):
        collection = '%s_data' % source
        if suffix:
            collection = '_'.join((collection, suffix))
        self.m[collection].drop()


    def _get_timestamps(self, created=True):
        now = datetime.now()
        timestamp = { 'modified' : now }
        if created:
            timestamp['created'] = now
        return timestamp

    def get_source_ids(self, source, type):
        collection = '%s_%s_id' % (source, type)
        return imap(self._clean, self.m[collection].find())

    # TODO: only produce source titles which aren't matched yet
    def get_titles_for_matching(self, source, status=('new', 'updated', None)):
        collection = '%s_title' % source
        if not status or status == ('all', ):
            titles = self.m[collection].find()
        else:
            titles = self.m[collection].find(
                { '_admin.status' : { '$in' : status } })
        for title in titles:
            yield self._doc_to_matcher(title)

    def get_title_for_matching(self, source, id):
        collection = '%s_title' % source
        return self._doc_to_matcher(self.m[collection].find_one(id))

    def _doc_to_matcher(self, title):
        re_title_fixer = re.compile(':[^:]*? (edition|cut)$')
        matcher = {
            'id' : title['_id'],
            'name' : re_title_fixer.sub('', title['name'].lower()),
            'year' : title['year'],
            'votes' : 'rating' in title and title['rating'].get('count') or 0,
            'alternate' : title.get('alternate'),
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
        return matcher

    def get_persons_for_matching(self, source,
                                 status=('new', 'updated', None)):
        collection = '%s_person' % source
        if not status or status == ('all', ):
            titles = self.m[collection].find()
        else:
            titles = self.m[collection].find(
                { '_admin.status' : { '$in' : status } })
        for person in titles:
            matcher = {
                'id' : person['_id'],
                'name' : person['name'],
                'titles' : frozenset(),
                'href' : person['href'],
            }
            yield matcher

    def get_person_for_matching(self, source, id):
        collection = '%s_person' % source
        return self._doc_to_matcher(self.m[collection].find_one(id))

    def get_matches(self, type='title', status=('new', 'updated', None)):
        collection = '%s_match' % type
        if not status or status == ('all', ):
            matches = self.m[collection].find()
        else:
            matches = self.m[collection].find(
                { '_admin.status' : { '$in' : status } })
        return imap(self._clean, matches)

    def get_matches_from_things(self, type='title'):
        for thing in self.m[type].find():
            match = { 'id' : thing['_id'] }
            match.update(thing['alternate'])
            yield match

    def mark_source_title_unmatched(self, source, id):
        self.update_source_status(source, 'title', id, 'unmatched')

    def update_source_status(self, source, type, id, status):
        collection = '%s_%s' % (source, type)
        fields = { '$set' : { '_admin.status' : status } }
        self.m[collection].update({ '_id' : id }, fields,
                                  upsert=False, multi=False)
        return id

    def update_match_status(self, type, id, status):
        collection = '%s_match' % type
        fields = { '$set' : { '_admin.status' : status } }
        self.m[collection].update({ '_id' : id }, fields,
                                  upsert=False, multi=False)

    def consume_match(self, match, type='title'):
        collection = '%s_match' % type
        doc = dict(match_iter(match))
        if not doc:
            raise Exception('Match is empty of ids')
        if not match.get('id'):
            doc['_id'] = self._get_seq_id(type)
            doc['_admin'] = { 'status' : 'new' }
        else:
            doc['_id'] = match['id']
            doc['_admin'] = { 'status' : 'updated' }

        self.m[collection].update({ '_id' : doc['_id'] },
                                  doc, upsert=True, multi=False)
        for source_name, source_id in match_iter(match):
            self.update_source_status(source_name, type,
                                    source_id, 'matched')
        return doc['_id']

    def consume_matches(self, producer, type='title'):
        #count = 0
        #for title in self.m.netflix_title.find({ '_admin.status' : 'matched' }, { '_id' : 1 }):
            #match_entry = self.m.title_match.find_one({ 'netflix' : title['_id'] }, { '_id' : 1 })
            #if not match_entry:
                #self.m.netflix_title.update(title, { '$set' : {
                    #'_admin' : { 'status' : 'updated' } } })
                #count += 1
        #print count
        #return

        return map(partial(self.consume_match, type=type), producer)

    def consume_source_titles(self, producer, source_name):
        start = time.time()
        collection = '%s_title' % source_name
        for title_in in imap(self._jsonify, producer):
            title_new = dict([ (k, v) for k, v in title_in.items() if
                           k not in ('id', 'noinsert') ])
            title_old = self.m[collection].find_one(
                { '_id' : title_in['id'] })
            if not title_old:
                title_new['_admin'] = { 'status' : 'new' }
                title_new['_id'] = title_in['id']
                self.m[collection].insert(title_new)
            else:
                for k in ('_admin', '_id'):
                    if k in title_old:
                        del title_old[k]
                if title_old != title_new:
                    title_new['_admin'] = { 'status' : 'updated' }
                    self.m[collection].update(
                        { '_id' : title_in['id'] },
                        { '$set' : title_new },
                        upsert=False, multi=False)

        log.info('Finished importing titles (%ss)' % str(time.time() - start))

    def get_source_titles(self, source_name, min_votes=0):
        collection = '%s_title' % source_name
        return imap(self._clean,
            self.m[collection].find(sort=[('_id', pmongo.ASCENDING)]))

    def get_source_title_by_id(self, name, id):
        collection = '%s_title' % name
        doc = self.m[collection].find_one({'_id' : id })
        return self._clean(doc) if doc else None

    def get_titles(self):
        return imap(self._clean, self.m.title.find())

    def get_title_ratings(self):
        return imap(self._clean,
                              self.m.title.find(fields={'rating' : 1}))
    
    def consume_source_persons(self, producer, source_name):
        start = time.time()
        collection = '%s_person' % source_name
        clean_producer = imap(self._jsonify, producer)
        for person_in in imap(self._clean, clean_producer):
            person = dict([ (k, v) for k, v in person_in.items() if
                            not k == 'noinsert' ])
            has_person = self.m[collection].find_one(
                { '_id' : person['_id'] })
            if not has_person and not person_in.get('noinsert'):
                self.m[collection].insert(person)
            else:
                self.m[collection].update(
                    { '_id' : person['_id'] },
                    { '$set' : person },
                    upsert=False, multi=False)
        log.info('Finished importing persons (%ss)' % str(time.time() - start))

    def get_person_role_titles(self):
        for person in self.m.person.find(fields={ '_id' : 1 },
                                         sort=[('_id', pmongo.ASCENDING)]):
            person_id = person['_id']
            for group in self._role_groups:
                titles = []
                group_key = '.'.join((group, 'person_id'))
                title_fields = { 'year' : 1, 'rating' : 1,
                                  group : 1 }
                for title in self.m.title.find({ group_key : person_id },
                                               fields=title_fields):
                    for member in title[group]:
                        if member.get('person_id') == person_id:
                            titles.append({
                                'rating' : title['rating'],
                                'year' : title['year'],
                                'billing' : member.get('billing'),
                            })
                        break
                if titles:
                    titles.sort(key=itemgetter('year'))
                    yield (person_id, group), titles
    
    def get_persons(self):
        return imap(self._clean, self.m.person.find())

    def get_title_persons(self):
        for title in self.m.title.find():
            persons = []
            for producers in title['production'].values():
                persons.extend(producers)
            persons.extend(title.cast)
            yield persons

    def get_source_person_by_id(self, name, id):
        collection = '%s_person' % name
        doc = self.m[collection].find_one({'_id' : id })
        return self._clean(doc) if doc else None

    def _get_seq_id(self, collection):
        return self.m.seq.find_and_modify({ '_id' : collection },
                                          { '$inc' : { 'seq' : 1 } },
                                          upsert=True,
                                          new=True)['seq']

    def _clean(self, thing, key='id'):
        if '_admin' in thing:
            del thing['_admin']
        if '_id' in thing and not key in thing:
            thing[key] = thing['_id']
            del thing['_id']
        elif key in thing and not '_id' in thing:
            thing['_id'] = thing[key]
            del thing[key]
        return thing

    def _get_keys_spec(self, keys_dict, root_key='key'):
        dotted_keys = self._dict_to_dot(keys_dict, root_key)
        return dict([ [k, { '$in' : ( v, None ) }] for
                      k, v in dotted_keys.items() ])

    def _dict_to_dot(self, match, root_key=''):
        dot_match = {}
        for k, v in match.items():
            dot_key = '.'.join((root_key, k)).lstrip('.')
            if isinstance(v, dict):
                sub_match = self._dict_to_dot(v, dot_key)
                for sk, sv in sub_match.items():
                    dot_match[sk] = sv
            else:
                dot_match[dot_key] = v
        return dot_match

    def _jsonify(self, doc):
        if isinstance(doc, dict):
            for k in doc.keys():
                if isinstance(k, int):
                    doc[str(k)] = self._jsonify(doc[k])
                    del doc[k]
                else:
                    doc[k] = self._jsonify(doc[k])
        elif isinstance(doc, tuple):
            doc = [ v for v in doc ]
            doc = self._jsonify(doc)
        elif isinstance(doc, list):
            for i in range(len(doc)):
                doc[i] = self._jsonify(doc[i])
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
