import logging

from sqlalchemy import or_, create_engine, orm, func

from filmdata.sinks.sa import model
from filmdata.sinks.sa import meta
from filmdata import config

log = logging.getLogger(__name__)

class SaSink:

    _row_dicter = staticmethod(lambda row, keys, offset:
                               dict(zip(keys, row[offset:])))
    _data_cols = ('rating', 'votes')

    def __init__(self):
        model.init_model(create_engine(config.sqlalchemy.url))
        self.__s = None #sqlalchemy session

    def setup(self):
        log.info("Dropping all tables in the DB")
        meta.metadata.drop_all(bind=meta.engine)
        log.info("Creating all tables in the DB")
        meta.metadata.create_all(bind=meta.engine)

    def install(self):
        log.info("Creating all tables in the DB")
        meta.metadata.create_all(bind=meta.engine)

    def consume_roles(self, producer):
        i = 0
        for title, role, person in producer:
            i = self.__db_iter(i)
            title_model = self.__get_model(model.Title, title)
            person_model = self.__get_model(model.Person, person)

            role_def = {
                'person_id' : person_model.person_id,
                'title_id' : title_model.title_id,
                'type' : role['type']
            }

            if person_model.person_id and title_model.title_id:
                role_model = self.__get_model(model.Role, role_def)
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
                self.__s.add(role_model)
            else:
                not title_model.title_id and self.__s.add(title_model)
                not person_model.person_id and self.__s.add(person_model)

        self.__db_close()

    def consume_data(self, producer):
        i = 0
        for title, data in producer:
            i = self.__db_iter(i)

            data_name = data[0]
            data_cols = data[1]
            data_class = model.source[data_name]
            primary_key = '_'.join(('data', data_name, 'id'))
            title_model = self.__get_model(model.Title, title)
            if not title_model.title_id:
                aka_model = self.__s.query(model.AkaTitle)\
                        .join(model.AkaTitle.title)\
                        .filter(model.AkaTitle.name == title['name'])\
                        .filter(or_(model.AkaTitle.year == title['year'],
                                    model.Title.year == title['year']))\
                        .first()
                if not aka_model or not aka_model.aka_title_id:
                    #raise Exception('Title not found: %s for data %s' % (str(title), str(data)))
                    continue
                title_id = aka_model.title_id
            else:
                title_id = title_model.title_id

            data_cols['title_id'] = title_id
            data_model = self.__get_model(data_class, data_cols,
                                          { 'title_id' : title_id })

            if not getattr(data_model, primary_key):
                self.__s.add(data_model)
            else:
                for k, v in data_cols.iteritems():
                    setattr(data_model, k, v)

        self.__db_close()

    def consume_aka_titles(self, producer):
        i = 0
        for title, aka in producer:
            i = self.__db_iter(i)

            title_model = self.__get_model(model.Title, title)
            if not title_model.title_id:
                continue
            else:
                aka['title_id'] = title_model.title_id
                aka_model = self.__get_model(model.AkaTitle, aka)
            if not aka_model.aka_title_id:
                self.__s.add(aka_model)
        self.__db_close()

    def consume_metric(self, producer, name, type=None):

        tbl_model = model.metric[name]

        self.__db_open()

        if type:
            self.__s.query(tbl_model).filter(tbl_model.type == type).delete()
        else:
            self.__s.execute("truncate metric_%s restart identity" % (name))
        
        self.__db_close()

        i = 0
        for row in producer:
            i = self.__db_iter(i)
            if type and 'type' not in row:
                row['type'] = type
            self.__s.add(tbl_model(**row))

        self.__db_close()

    def get_titles_rating(self, min_votes=0):
        self.__db_open()

        data_keys, data_cols = zip(*model.source.get_sa_cols(self._data_cols))
        rows = self.__s.query(model.Title.title_id,
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

        self.__db_close()
        return titles_rating

    def get_persons_role_titles(self):
        self.__db_open()

        data_keys, data_cols = zip(*model.source.get_sa_cols(self._data_cols))
        rows = self.__s.query(model.Role.person_id,
                              model.Role.type,
                              model.Role.title_id,
                              model.Title.year,
                              model.Role.billing,
                              *data_cols
                             )\
                       .join(model.Title, *model.source.values())\
                       .filter(model.culler.votes >= 4000)\
                       .all()

        self.__db_close()

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

    def __get_model(self, model_class, row, search=None):
        if not search:
            search = dict(row)
        query = self.__s.query(model_class).filter_by(**search)
        model_db = query.first()
        return model_db if model_db else model_class(**row)

    def __get_number_model(self, title_key, imdb_rating, imdb_votes):
        title_db = self.__find_title_by_key(title_key)
        if title_db:
            if str(imdb_votes) != str(title_db.imdb_votes):
                title_db.imdb_rating = imdb_rating
                title_db.imdb_votes = imdb_votes
                title_db.changed = True
            else:
                title_db.changed = False
            title_model = title_db
        else:
            title_model = model.Title(title_key[0], title_key[1], title_key[2],
                                      imdb_rating, imdb_votes)
            title_model.changed = True
        return title_model

    def __db_iter(self, i):
        if i == 1000 or not self.__s:
            if self.__s:
                self.__db_close()
            self.__db_open()
            i = 1
        else:
            i += 1
        return i

    def __db_open(self):
        self.__s = meta.Session()

    def __db_close(self):
        self.__s.commit()
        self.__s.close()
        self.__s.connection().detach()
        self.__s = None
