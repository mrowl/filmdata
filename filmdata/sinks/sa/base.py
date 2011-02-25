import logging

from sqlalchemy import or_, create_engine, orm, func

from filmdata.sinks.sa import model
from filmdata.sinks.sa import meta
from filmdata import config

log = logging.getLogger(__name__)

class SaSink:
    __data_classes = {
        'imdb' : model.DataImdb,
        'netflix' : model.DataNetflix,
    }

    def __init__(self):
        model.init_model(create_engine(config.get('sqlalchemy', 'url')))
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

    def consume_numbers(self, producer):
        i = 0
        for title, data in producer:
            i = self.__db_iter(i)

            data_name = data[0]
            data_cols = data[1]
            data_class = self.__data_classes[data_name]
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

    def consume_metric(self, producer, tbl_name, type=None):

        classes = {
            'metric_title' : model.MetricTitle,
            'metric_person_role' : model.MetricPersonRole,
        }

        tbl_model = classes[tbl_name]

        self.__db_open()

        if type:
            self.__s.query(tbl_model).filter(tbl_model.type == type).delete()
        else:
            self.__s.execute("truncate %s restart identity" % (tbl_name))
        
        self.__db_close()

        i = 0
        for row in producer:
            i = self.__db_iter(i)
            if type and 'type' not in row:
                row['type'] = type
            self.__s.add(tbl_model(**row))

        self.__db_close()

    def get_titles_rating(self):
        self.__db_open()

        dict_maker = lambda t: {
            'title_id' : t.title_id,
            'imdb_rating' : t.data_imdb.rating,
            'imdb_votes' : t.data_imdb.votes,
            'netflix_rating' : t.data_netflix.rating,
        }

        #.filter(model.Title.type.in_(('film',)))\
        titles = self.__s.query(model.Title)\
                         .options(orm.contains_eager(model.Title.data_imdb))\
                         .options(orm.contains_eager(model.Title.data_netflix))\
                         .join(model.Title.data_imdb)\
                         .join(model.Title.data_netflix)\
                         .filter(model.DataImdb.votes > 4000)\
                         .all()

        titles_rating =  [ dict_maker(t) for t in titles ]
        self.__db_close()
        return titles_rating

    def get_persons_role_titles_agg(self):
        self.__db_open()

        orm.util.class_mapper(model.Person).add_properties({
            "titles_count" : orm.column_property(
                func.count(model.Title.title_id).label("titles_count")
            ),
            "imdb_rating_sum" : orm.column_property(
                func.sum(model.DataImdb.rating).label("imdb_rating_sum")
            ),
            "imdb_votes_sum" : orm.column_property(
                func.sum(model.DataImdb.votes).label("imdb_votes_sum")
            ),
            "netflix_rating_sum" : orm.column_property(
                func.sum(model.DataNetflix.rating).label("netflix_rating_sum")
            ),
        })

        dict_maker = lambda p: {
            'person_id' : p.person_id,
            'role_type' : p.type,
            'imdb_rating_sum' : p.imdb_rating_sum,
            'imdb_votes_sum' : p.imdb_votes_sum,
            'netflix_rating_sum' : p.netflix_rating_sum,
            'titles_count' : p.titles_count,
        }

        persons = self.__s.query(model.Person.person_id,
                                 model.Person.titles_count,
                                 model.Person.imdb_rating_sum,
                                 model.Person.imdb_votes_sum,
                                 model.Person.netflix_rating_sum,
                                 model.Role.type)\
                          .join(model.Role)\
                          .join(model.Title)\
                          .join(model.DataImdb)\
                          .join(model.DataNetflix)\
                          .filter(model.DataImdb.votes >= 4000)\
                          .filter(model.Title.type == 'film')\
                          .filter(or_(model.Role.type == 'director', 
                                      model.Role.billing <= 8))\
                          .group_by(model.Person.person_id)\
                          .group_by(model.Role.type)\
                          .having(model.Person.titles_count >= 4)\
                          .all()

        self.__db_close()

        return [ dict_maker(p) for p in persons ]

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
