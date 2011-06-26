import logging
import re
import time
import itertools
from functools import partial
from decimal import Decimal
from unicodedata import normalize
import Levenshtein

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

    def match_source_titles(self, major_name, minor_name):
        start = time.time()
        major = dict([ (t['key'], t) for t in 
                       self.get_titles_for_matching(major_name) ])
        minor = dict([ (t['key'], t) for t in
                       self.get_titles_for_matching(minor_name) ])
        print time.time() - start

        minor_to_major = {}
        major_to_minor = {}

        def cleanup():
            for k in major_to_minor.iterkeys():
                if k in major:
                    del major[k]
            for k in minor_to_major.iterkeys():
                if k in minor:
                    del minor[k]

        # lower is better
        def score_pair(one, two):
            score = 0

            if one['name'] != two['name']:
                one_names = set((one['name'], )) if not 'aka' in one else one['aka']
                two_names = set((two['name'], )) if not 'aka' in two else two['aka']
                if not len(one_names & two_names) > 0:
                    pairs = [ (a, b) for a in one_names for b in two_names ] 
                    for a, b in pairs:
                        if a in b or b in a:
                            score = 1
                            break
                    else:
                        score += min([ Levenshtein.distance(n1, n2) for 
                                       n1 in one_names for n2 in two_names ])
            if one['year'] != two['year']:
                score += one['year'] - two['year']
            if 'director' in one and 'director' in two:
                one_len = len(one['director'])
                two_len = len(two['director'])
                if one_len == two_len:
                    score += 2 * len(one['director'] - two['director'])
                elif one_len == 0 or two_len == 0:
                    score += 2
                else:
                    smaller = min(one_len, two_len)
                    if one_len < two_len:
                        score += 2*len(one['director'] - two['director'])
                    else:
                        score += 2*len(two['director'] - one['director'])
            return score

        def best_match(title, major_keys=None, minor_keys=None):
            if major_keys is not None:
                if len(major_keys) == 1:
                    return major_keys[0]
                else:
                    guesses = [ major[k] for k in major_keys ]
            elif minor_keys is not None:
                if len(minor_keys) == 1:
                    return minor_keys[0]
                else:
                    guesses = [ minor[k] for k in minor_keys ]
            else:
                return None

            best_yet = (9999, None)
            for guess in guesses:
                cur_score = score_pair(title, guess)
                if cur_score < best_yet[0]:
                    best_yet = (cur_score, guess['key'])
            return best_yet[1]


        def index_matcher(key_gen):
            index = {}
            for k, t in major.iteritems():
                ikeys = key_gen(t)
                for ikey in ikeys:
                    if ikey in index:
                        index[ikey].append(k)
                    else:
                        index[ikey] = [ k, ]
                    
            for i, title in enumerate(minor.itervalues()):
                ikeys = key_gen(title)
                for ikey in ikeys:
                    if ikey in index:
                        major_key = best_match(title, index[ikey])
                        if major_key in major_to_minor:
                            # possible duplicates in the minor list
                            # see which of the these two keys is better
                            minor_key = best_match(major[major_key],
                                                   minor_keys=[title['key'],
                                                               major_to_minor[major_key]])
                            chosen = 'current' if minor_key == title['key'] else 'prev'
                            print "Duplicate in minor list? Both matching to same major title."
                            print "Selected %s (%s) in the end" % (str(minor_key), chosen)
                            print "Second minor title (current one):"
                            print title
                            print "First minor title:"
                            print minor[major_to_minor[major_key]]
                            print "The major title to which they are matching"
                            print major[major_key]
                            print ""
                        else:
                            minor_key = title['key']
                        minor_to_major[minor_key] = major_key
                        major_to_minor[major_key] = minor_key
            cleanup()

        def key_gen_aka(title):
            if 'aka' in title:
                years = set((title['year'], ))
                if 'aka_year' in title:
                    years |= title['aka_year'] | set((title['year'] + 1,
                                                      title['year'] - 1))
                names = title['aka'] | set((title['name'], ))
                return [ (year, a) for year in years for a in names ]
            else:
                return ((title['year'], title['name']), )

        re_alpha = re.compile('[^A-Za-z0-9\_]')
        fuzz = lambda s: re_alpha.sub('', normalize('NFKD', s))
        key_gen_fuzzy_names = lambda t: [ (p[0], fuzz(p[1])) for
                                    p in key_gen_aka(t) ]
        key_gen_fuzzy_dirs = lambda t: [ (t['year'], fuzz(d)) for
                                         d in t['director'] ] if 'director' in t else []

            #for i, minor_title in enumerate(minor):
        index_matcher(lambda t: ((t['name'], t['year']), ))
        index_matcher(key_gen_aka)
        index_matcher(key_gen_fuzzy_names)
        index_matcher(key_gen_fuzzy_dirs)
        #index_matcher(lambda t: t['name'])

        print len(minor_to_major)
        print time.time() - start
        consume_title_matches(minor_to_major.iteritems(), minor_name, major_name)

    def merge_titles(self, netflix=None, flixster=None, imdb=None):
        pass

    def consume_title_matches(self, matches, key_source, value_source):
        key_ref = '_'.join((key_source, 'title_id'))
        key_collection = '%s_title' % key_source
        value_ref = '_'.join((key_value, 'title_id'))
        value_collection = '%s_title' % value_source
        self.m.title.ensure_index(((key_ref,
                                    pmongo.ASCENDING),
                                   (value_ref,
                                    pmongo,ASCENDING)), unique=True)
        for k, v in matches:
            already_matched = self.m.title.find({ key_ref : k, value_ref : v })
            # TODO: check if just one key is in there, may be a third source or
            # something
            if not already_matched:
                key_title = self.m[key_collection].find({ '_id' : k })
                value_title = self.m[value_collection].find({ '_id' : v })
                merge_args = {
                    key_source : key_title,
                    value_source : value_title,
                }
                new_title = self.merge_titles(merge_args)
                self.m.title.insert(new_title)




    # TODO: only produce source titles which aren't matched yet
    def get_titles_for_matching(self, source):
        collection = '%s_title' % source
        re_title_fixer = re.compile(':[^:]*? (edition|cut)$')
        for title in self.m[collection].find():
            matcher = {
                'key' : title['_id'],
                'name' : re_title_fixer.sub('', title['name'].lower()),
                'year' : title['year'],
            }
            if 'aka' in title and title['aka'] is not None and len(title['aka']) > 0:
                matcher['aka'] = frozenset([ a['name'].lower() for a in
                                             title['aka'] ])
                matcher['aka_year'] = frozenset([ int(a['year']) for a in
                                                   title['aka'] if a['year'] ])
            else:
                akas = matcher['name'].split(' / ')
                if len(akas) > 0:
                    matcher['aka'] = frozenset(akas)
            if 'production' in title and 'director' in title['production']:
                matcher['director'] = frozenset([ d['name'].lower() for d in
                                                  title['production']['director'] ])
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
