from pymongo import Connection
from pymongo.code import Code
from collections import defaultdict
from hashlib import md5
from filmlust.lib.stats import memoize
import re

class MongoSink:

    def __init__(self):
        self.__conn = Connection()
        self.__db = self.__conn.filmlust
        self.__t = self.__db.titles
        self.__p = self.__db.persons
        self.__safe = False

    def consume_titles(self, producer):
        docs = []
        i = 0
        docs_per_insert = 200
        for title in producer:
            i += 1
            title['_id'] = self.__get_title_id(title)
            docs.append(title)
            if i == docs_per_insert:
                self.__t.insert(docs, True, self.__safe)
                docs = []
                i = 0

    def consume_numbers(self, producer):
        for number in producer:
            self.__update({ '_id' : self.__get_title_id(number[0]) },
                          { '$set' : { number[1][0] : number[1][1] } })

    def consume_roles(self, producer):
        for role in producer:
            self.__update({ '_id' : self.__get_title_id(role[0]) },
                          { '$push' : { role[1] : role[2] } })

    @memoize
    def get_person_average(self, role='director'):
        map = Code("function () {"
                   "    var rating = this.imdb.rating;"
                   "    var i = this." + role + ".length;"
                   "    while (i--) {"
                   "        emit(this." + role + "[i], this.imdb.rating);"
                   "    }"
                   "}")
        reduce = Code("function (key, values) {"
                      "    var sum = 0;"
                      "    var i = values.length;"
                      "    while (i--) {"
                      "        sum += values[i];"
                      "    }"
                      "    return sum;"
                      "}")
        #query = { role : { '$in' : ['Nolan, Christopher (I)', re.compile('^Hitchcock, Alfred')] } }
        query = { role : { '$exists' : 'true' } }
        mr_result =  self.__t.map_reduce(map, reduce, query=query).find(timeout=False)
        for doc in mr_result:
            count = self.__t.find({ role : doc['_id'] }).count()
            if count > 4:
                doc['avg'] = doc['value'] / count
                print doc

    @memoize
    def get_person_groups(self, role='dirctor'):
        reduce = Code("function (obj, prev) {"
                      "    while (i--) {"
                      "        prev.sum += obj.imdb.rating;"
                      "        prev.count++;"
                      "    }"
                      "}")
        groups = self.__t.group({ role : 'true' },
                                { role : { '$exists' : 'true' } },
                                { 'sum' : 0, 'count' : 0 }, reduce)
        print groups

    @memoize
    def get_average(self):
        return self.get_sum() / self.get_count()

    @memoize
    def get_sum(self):
        map = Code("function () {"
                   "    emit(0, this.imdb.rating);"
                   "}")
        reduce = Code("function (key, values) {"
                      "    var sum = 0;"
                      "    var i = values.length;"
                      "    while (i--) {"
                      "        sum += values[i];"
                      "    }"
                      "    return sum;"
                      "}")
        return self.__t.map_reduce(map, reduce).find_one(0)['value']

    @memoize
    def get_count(self):
        return self.__t.count()

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

    def __group_roles(self, roles):
        groups = defaultdict(list)
        for type, person in roles:
            groups[type].append(person[0])
        return groups
