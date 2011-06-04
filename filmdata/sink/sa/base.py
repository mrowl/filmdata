import logging, itertools

from sqlalchemy import or_, create_engine, exc as sa_exc

from filmdata.sink.sa import model
from filmdata.sink.sa import meta
from filmdata import config

log = logging.getLogger(__name__)

class SaSink:

    _row_dicter = staticmethod(lambda row, keys, offset:
                               dict(zip(keys, row[offset:])))
    _data_cols = ('rating', 'votes')

    def __init__(self):
        model.init_model(create_engine(config.sqlalchemy.url))
        self._db_open()
        self._match_counts = {}
        #self._profile = hotshot.Profile('hotshot.txt')

    def setup(self):
        log.info("Dropping all tables in the DB")
        meta.metadata.drop_all(bind=meta.engine)
        log.info("Creating all tables in the DB")
        meta.metadata.create_all(bind=meta.engine)

    def install(self):
        log.info("Creating all tables in the DB")
        meta.metadata.create_all(bind=meta.engine)

    def _consumer(consume_fn):
        def wrapper(self, producer):
            for i, row in enumerate(producer):
                if i % 100 == 0:
                    print "%d total; %d matches  :  %s" % (i,
                        sum(self._match_counts.values()),
                        str(self._match_counts))
                try:
                    consume_fn(self, row)
                except sa_exc.SQLAlchemyError as e:
                    log.warn("Row didn't consume: %s\n%s" % (str(row), str(e)))
        return wrapper

    @_consumer
    def consume_roles(self, producer):
        title, role, person = producer
        title_model = self._get_model(model.Title, title)
        person_model = self._get_model(model.Person, person)

        role_def = {
            'person_id' : person_model.person_id,
            'title_id' : title_model.title_id,
            'type' : role['type']
        }

        if person_model.person_id and title_model.title_id:
            role_model = self._get_model(model.Role, role_def)
        else:
            role_model = model.Role(**role_def)

        if not role_model.role_id:
            if role['billing'] == None:
                role_model.billing = None
            else:
                role_model.billing = min(int(role['billing']), 32767)
            role_model.character = role['character']
            role_model.person = person_model
            role_model.title = title_model
            self._s.add(role_model)
        else:
            not title_model.title_id and self._s.add(title_model)
            not person_model.person_id and self._s.add(person_model)

    @_consumer
    def consume_data(self, producer):
        title, data = producer
        source_name = data[0]
        source_cols = data[1]
        source_model = model.source[source_name]
        source_primary_key = '_'.join(('source', source_name, 'id'))

        source_obj = None
        if 'key' in source_cols:
            title_id = None
            source_obj = self._get_model(source_model, source_cols,
                                         { 'key' : source_cols['key'] })
        if not source_obj or not getattr(source_obj, source_primary_key):
            title_id = self._get_source_title_id(title)
            if title_id is None:
                log.debug('No match for "%s" with data "%s"' %
                          (str(title), str(data)))
                return
            source_obj = self._get_model(source_model, source_cols,
                                         { 'title_id' : title_id })
            if not getattr(source_obj, source_primary_key):
                source_obj.title_id = title_id
                self._s.add(source_obj)
        else:
            log.debug('Matched "%s" with source key: %s' %
                      (str(title), str(source_cols['key'])))

        for k, v in source_cols.iteritems():
            setattr(source_obj, k, v)

    @_consumer
    def consume_aka_titles(self, producer):
        title, aka = producer
        title_model = self._get_model(model.Title, title)
        if not title_model.title_id:
            return
        aka['title_id'] = title_model.title_id
        aka_model = self._get_model(model.AkaTitle, aka)
        if not aka_model.aka_title_id:
            self._s.add(aka_model)

    def consume_metric(self, producer, name, type=None):

        tbl_model = model.metric[name]

        if type:
            self._s.query(tbl_model).filter(tbl_model.type == type).delete()
        else:
            self._s.execute("truncate metric_%s restart identity" % (name))

        for row in producer:
            if type and 'type' not in row:
                row['type'] = type
            self._s.add(tbl_model(**row))

    def get_titles_rating(self, min_votes=0):
        data_keys, data_cols = zip(*model.source.get_sa_cols(self._data_cols))
        rows = self._s.query(model.Title.title_id,
                              *data_cols
                             )\
                       .join(*model.source.values())\
                       .filter(model.culler.votes >= min_votes)\
                       .all()

        titles_rating = []
        for row in rows:
            title = { 'title_id' : row[0] }
            title.update(self._row_dicter(row, data_keys, 1))
            titles_rating.append(title)

        return titles_rating

    def get_persons_role_titles(self):
        data_keys, data_cols = zip(*model.source.get_sa_cols(self._data_cols))
        rows = self._s.query(model.Role.person_id,
                             model.Role.type,
                             model.Role.title_id,
                             model.Title.year,
                             model.Role.billing,
                             *data_cols
                            )\
                      .join(model.Title, *model.source.values())\
                      .filter(model.culler.votes >= 4000)\
                      .all()

        person_roles = {}
        for r in rows:
            person_key = ( r[0], r[1] )
            new_title = {
                'id' : r[2],
                'year' : r[3],
                'billing' : r[4],
            }
            new_title.update(self._row_dicter(r, data_keys, 5))
            if person_key in person_roles:
                person_roles[person_key].append(new_title)
            else:
                person_roles[person_key] = [ new_title ]

        return person_roles

    def _get_source_title_id(self, title):
        match_waterfall = [ 
            (self._match_title_simple, 'title', False, False),
            (self._match_title_simple, 'aka_title', False, False),
            (self._match_title_simple, 'title', True, True),
            (self._match_title_simple, 'aka_title', True, True),
            (self._match_title_levenshtein, 'title', False, True),
            (self._match_title_levenshtein, 'aka_title', False, True),
            #(self._match_title_trigram, 'aka_title_merged', True),
        ]
        name = title['name']
        year = int(title['year'])
        for func, table, year_fuzziness, lower in match_waterfall:
            match = func(name, self._get_year_filter(year, year_fuzziness),
                         table, lower)
            if match:
                match_key = (func.__name__, table, year_fuzziness, lower)
                log.debug('%s matched (%s, %s) "%s" to "%s"' %
                          (func.__name__, table, year, name, match[1]))
                if not match_key in self._match_counts:
                    self._match_counts[match_key] = 0
                self._match_counts[match_key] += 1
                return match[0]
        return None

    def _get_year_filter(self, year, fuzzy=False):
        if fuzzy:
            return 'year in (%s)' % ','.join(map(str, range(year - 1,
                                                            year + 2)))
        return 'year=%s' % str(year)

    def _match_title_simple(self, name, year_filter, table='title',
                            lower=False):
        name_selector = 'name'
        if table == 'title':
            stmt = [
                'select title_id, name',
                'from title',
                'where %s' % year_filter,
            ]
        elif table == 'aka_title':
            stmt = [
                'select at.title_id, at.name',
                'from aka_title at',
                'join title t on at.title_id=t.title_id',
                'where (at.%s or t.%s) ' % (year_filter, year_filter),
            ]
            name_selector = 'at.name'
        name_filter = 'and lower(%s)=:name' if lower else 'and %s=:name'
        stmt.append(name_filter % name_selector)
        return self._s.query('title_id', 'name')\
                      .from_statement(' '.join(stmt))\
                      .params(name=name.lower() if lower else name).first()

    def _match_title_levenshtein(self, name, year_filter, table='title',
                                 lower=True):
        try:
            best_match = self._s.query('title_id', 'name')\
                    .from_statement(
                        'select title_id, name, ' +
                        'levenshtein(lower(name), :name) / length(:name) as dist ' +
                        'from %s ' % table +
                        'where %s ' % year_filter +
                        'and levenshtein(lower(name), :name) / cast(length(:name) as float) < .09 ' +
                        'order by dist asc')\
                    .params(name=name.lower())\
                    .first()
        except sa_exc.NotSupportedError:
            best_match = None
            log.info('Levenshtein not supported on title: %s' %
                      name)
        return best_match

    def _match_title_trigram(self, name, year_filter, table='title',
                             lower=False):
        try:
            best_match = self._s.query('title_id', 'name')\
                    .from_statement(
                        'select title_id, name, ' +
                        'similarity(name, :name) as score ' +
                        'from %s ' % table +
                        'where %s ' % year_filter +
                        'and name % :name ' +
                        'order by score desc')\
                    .params(name=name)\
                    .first()
        except sa_exc.NotSupportedError:
            best_match = None
            log.debug('Trigrams not supported on title: %s' %
                      name)
        return best_match

    def _get_model(self, model_class, row, search=None):
        if not search:
            search = dict(row)
        query = self._s.query(model_class).filter_by(**search)
        model_db = query.first()
        return model_db if model_db else model_class(**row)

    def _db_iter(self, i):
        if i == 1000 or not self._s:
            if self._s:
                self._db_close()
            self._db_open()
            self._s.query('limit').from_statement(
                'select set_limit(:limit) as limit').params(limit=.8).first()
            i = 1
        else:
            i += 1
        return i

    def _db_open(self):
        self._s = meta.Session(autocommit=True)
        self._s.execute("select set_limit(.8)")

    def _db_close(self):
        self._s.commit()
        self._s.close()
        self._s.connection().detach()
        self._s = None
