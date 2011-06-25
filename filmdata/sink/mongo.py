import logging
import time
import itertools
from functools import partial
from decimal import Decimal

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

    def consume_title_akas(self, producer, source_name=None):
        start = time.time()
        collection = 'title' if source_name is None else 'title_%s' % source_name
        self.m[collection].ensure_index('key', unique=True)
        for aka in itertools.imap(self._clean_document, producer):
            self.m[collection].update(
                { 'key' : aka['title']['key'] },
                { '$addToSet' : { 'aka' : aka['aka'] } },
                upsert=False, multi=False)

        log.info('Finished importing akas (%ss)' % str(time.time() - start))

    def match_source_titles(self, source_one, source_two):
        pass

    def get_titles_for_matching(source):
        collection = '%s_title' % source
        self.m[collection].find()

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

    def consume_numbers(self, producer, source_name=None):
        for number in producer:
            self.__update({ '_id' : self.__get_title_id(number[0]) },
                          { '$set' : { number[1][0] : number[1][1] } })

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

    def get_titles(self, source_name, min_votes=0):
        collection = 'title' if source_name is None else 'title_%s' % source_name
        return self.m[collection].find(sort=[('key', pmongo.ASCENDING)])

    #@memoize
    #def get_person_average(self, role='director'):
        #map = Code("function () {"
                   #"    var rating = this.imdb.rating;"
                   #"    var i = this." + role + ".length;"
                   #"    while (i--) {"
                   #"        emit(this." + role + "[i], this.imdb.rating);"
                   #"    }"
                   #"}")
        #reduce = Code("function (key, values) {"
                      #"    var sum = 0;"
                      #"    var i = values.length;"
                      #"    while (i--) {"
                      #"        sum += values[i];"
                      #"    }"
                      #"    return sum;"
                      #"}")
        ##query = { role : { '$in' : ['Nolan, Christopher (I)', re.compile('^Hitchcock, Alfred')] } }
        #query = { role : { '$exists' : 'true' } }
        #mr_result =  self.__t.map_reduce(map, reduce, query=query).find(timeout=False)
        #for doc in mr_result:
            #count = self.__t.find({ role : doc['_id'] }).count()
            #if count > 4:
                #doc['avg'] = doc['value'] / count
                #print doc

    #@memoize
    #def get_person_groups(self, role='dirctor'):
        #reduce = Code("function (obj, prev) {"
                      #"    while (i--) {"
                      #"        prev.sum += obj.imdb.rating;"
                      #"        prev.count++;"
                      #"    }"
                      #"}")
        #groups = self.__t.group({ role : 'true' },
                                #{ role : { '$exists' : 'true' } },
                                #{ 'sum' : 0, 'count' : 0 }, reduce)
        #print groups

    #@memoize
    #def get_sum(self):
        #map = Code("function () {"
                   #"    emit(0, this.imdb.rating);"
                   #"}")
        #reduce = Code("function (key, values) {"
                      #"    var sum = 0;"
                      #"    var i = values.length;"
                      #"    while (i--) {"
                      #"        sum += values[i];"
                      #"    }"
                      #"    return sum;"
                      #"}")
        #return self.__t.map_reduce(map, reduce).find_one(0)['value']

    def _clean_document(self, doc):
        if isinstance(doc, dict):
            for k in doc.keys():
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
