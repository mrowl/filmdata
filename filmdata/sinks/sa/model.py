"""The application's model objects"""
#from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import orm
from sqlalchemy.ext.associationproxy import association_proxy
import sqlalchemy as sa

from filmdata.sinks.sa import meta
import datetime

TITLE_TYPES = ('film', 'tv')
ROLE_TYPES = ('director', 'actor', 'actress', 'producer', 'writer')

def init_model(engine):
    """Call me before using any of the tables or classes in the model"""
    meta.Session.configure(bind=engine)
    meta.engine = engine

class EnumIntType(sa.types.TypeDecorator):
    impl = sa.types.SmallInteger

    values = None

    def __init__(self, values=None):
        sa.types.TypeDecorator.__init__(self)
        self.values = values

    def process_bind_param(self, value, dialect):
        return None if value == None else self.values.index(value)

    def process_result_value(self, value, dialect):
        return None if value == None else self.values[value]

## Non-reflected tables may be defined and mapped at module level
person_table = sa.Table("person", meta.metadata,
    sa.Column("person_id", sa.types.Integer, primary_key=True),
    sa.Column("name", sa.types.Unicode(100), nullable=False, unique=True),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    )

title_table = sa.Table("title", meta.metadata,
    sa.Column("title_id", sa.types.Integer, primary_key=True),
    sa.Column("name", sa.types.Unicode(255), nullable=False),
    sa.Column("year", sa.types.SmallInteger, nullable=False),
    sa.Column("type", EnumIntType(TITLE_TYPES), nullable=False),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    sa.UniqueConstraint("name", "year", "type", name="title_info"),
    )

aka_title_table = sa.Table("aka_title", meta.metadata,
    sa.Column("aka_title_id", sa.types.Integer, primary_key=True),
    sa.Column("title_id", sa.types.Integer, sa.ForeignKey("title.title_id")),
    sa.Column("name", sa.types.Unicode(511), nullable=False),
    sa.Column("year", sa.types.SmallInteger, nullable=False),
    sa.Column("region", sa.types.Unicode(100), nullable=False),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    sa.UniqueConstraint("name", "year", "region", name="aka_title_info"),
    )

role_table = sa.Table("role", meta.metadata,
    sa.Column("role_id", sa.types.Integer, primary_key=True),
    sa.Column("person_id", sa.types.Integer, sa.ForeignKey("person.person_id")),
    sa.Column("title_id", sa.types.Integer, sa.ForeignKey("title.title_id")),
    sa.Column("type", EnumIntType(ROLE_TYPES), nullable=False),
    sa.Column("character", sa.types.Unicode(1023), nullable=True),
    sa.Column("billing", sa.types.SmallInteger, nullable=True, default=0),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    sa.UniqueConstraint("person_id", "title_id", "type", 
                        name="person_title_link"),
    )

data_imdb_table = sa.Table("data_imdb", meta.metadata,
    sa.Column("data_imdb_id", sa.types.Integer, primary_key=True),
    sa.Column("title_id", sa.types.Integer, sa.ForeignKey("title.title_id")),
    sa.Column("rating", sa.types.Numeric(asdecimal=True, scale=1)),
    sa.Column("votes", sa.types.Integer),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    sa.UniqueConstraint("title_id", name="imdb_title_link"),
    )

data_netflix_table = sa.Table("data_netflix", meta.metadata,
    sa.Column("data_netflix_id", sa.types.Integer, primary_key=True),
    sa.Column("title_id", sa.types.Integer, sa.ForeignKey("title.title_id")),
    sa.Column("key", sa.types.Integer),
    sa.Column("rating", sa.types.Numeric(asdecimal=True, scale=1)),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    sa.UniqueConstraint("title_id", name="netflix_title_link"),
    )

#data_freebase_table = sa.Table("data_freebase", meta.metadata,
    #sa.Column("data_freebase_id", sa.types.Integer, primary_key=True),
    #sa.Column("title_id", sa.types.Integer, sa.ForeignKey("title.title_id")),
    #sa.Column("freebase_key", UUID()),
    #sa.Column("created", sa.types.DateTime(), nullable=False,
              #default=datetime.datetime.now),
    #sa.Column("modified", sa.types.DateTime(), nullable=False,
              #default=datetime.datetime.now, onupdate=datetime.datetime.now),
    #sa.UniqueConstraint("title_id", name="freebase_title_link"),
    #)

metric_title_table = sa.Table("metric_title", meta.metadata,
    sa.Column("metric_title_id", sa.types.Integer, primary_key=True),
    sa.Column("title_id", sa.types.Integer, sa.ForeignKey("title.title_id")),
    sa.Column("imdb_rating", sa.types.Numeric(asdecimal=True)),
    sa.Column("imdb_votes", sa.types.Integer),
    sa.Column("imdb_bayes", sa.types.Numeric(asdecimal=True)),
    sa.Column("netflix_rating", sa.types.Numeric(asdecimal=True)),
    sa.Column("average_rating", sa.types.Numeric(asdecimal=True)),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    )

metric_person_role_table = sa.Table("metric_person_role", meta.metadata,
    sa.Column("metric_person_role_id", sa.types.Integer, primary_key=True),
    sa.Column("person_id", sa.types.Integer, sa.ForeignKey("person.person_id")),
    sa.Column("role_type", EnumIntType(ROLE_TYPES), nullable=False),
    sa.Column("titles_count", sa.types.Numeric(asdecimal=True)),
    sa.Column("imdb_votes_sum", sa.types.Numeric(asdecimal=True)),
    sa.Column("imdb_rating_avg", sa.types.Numeric(asdecimal=True)),
    sa.Column("netflix_rating_avg", sa.types.Numeric(asdecimal=True)),
    sa.Column("average_rating_avg", sa.types.Numeric(asdecimal=True)),
    sa.Column("imdb_rating_bayes", sa.types.Numeric(asdecimal=True)),
    sa.Column("netflix_rating_bayes", sa.types.Numeric(asdecimal=True)),
    sa.Column("average_rating_bayes", sa.types.Numeric(asdecimal=True)),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    )


class Person(object):
    titles = association_proxy('roles', 'title')

    def __init__(self, name=None):
        self.name = name

    def __repr__(self):
        return "<Person('%s')>" % (self.name)

    def as_dict(self):
        excludes = ['titles', 'roles', 'created', 'modified']
        d = {}
        for p in orm.util.object_mapper(self).iterate_properties:
            if p.key not in excludes:
                d[p.key] = getattr(self, p.key)
        return d

class Title(object):
    people = association_proxy('roles', 'person')

    def __init__(self, name=None, year=None, type=None):
        self.name = name
        self.year = year
        self.type = type

    def __repr__(self):
        return "<Title('%s','%s', '%s', '%s', '%s')>" % (self.name,
                                                          self.year,
                                                          self.type,
                                                         )

    def as_dict(self, include=None):
        excludes = ['titles', 'roles', 'created', 'data_netflix', 'data_imdb',
                    'modified', 'aka_titles']
        if include:
            del excludes[excludes.index(include)]

        d = {}
        for p in orm.util.object_mapper(self).iterate_properties:
            if p.key not in excludes:
                value = getattr(self, p.key)
                if p.key == 'data_imdb':
                    d['data_imdb_rating'] = value.rating
                    d['data_imdb_votes'] = value.votes
                elif p.key == 'data_netflix':
                    d['data_netflix_rating'] = value.rating
                else:
                    d[p.key] = value
        return d

class AkaTitle(object):

    def __init__(self, title_id=None, name=None, year=None, region=None):
        self.name = name
        self.year = year
        self.region = region
        self.title_id = title_id

    def __repr__(self):
        return "<AkaTitle('%s','%s', '%s', '%s')>" % (self.name,
                                                      self.year,
                                                      self.region,
                                                      self.title_id
                                                     )

class Role(object):

    def __init__(self, type=None, character=None, billing=None,
                 person_id=None, title_id=None):
        self.type = type
        self.character = character
        self.billing = billing
        self.person_id = person_id
        self.title_id = title_id

    def __repr__(self):
        return "<Role('%s')>" % (self.type, self.character, self.billing,
                                 self.person_id, self.title_id)

class DataImdb(object):

    def __init__(self, rating=None, votes=None, title_id=None, key=None):
        self.rating = rating
        self.votes = votes
        self.title_id = title_id
        self.key = key

    def __repr__(self):
        return "<DataImdb('%s','%s','%s')>" % (self.rating, self.votes,
                                               self.title_id)

class DataNetflix(object):

    def __init__(self, rating=None, title_id=None, key=None):
        self.rating = rating
        self.title_id = title_id
        self.key = key

    def __repr__(self):
        return "<DataNetflix('%s','%s','%s')>" % (self.rating, self.title_id,
                                                  self.key)

class MetricTitle(object):

    def __init__(self, **kwargs):
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    def __repr__(self):
        return "<MetricTitle('%s','%s','%s')>" % (self.title_id)

class MetricPersonRole(object):

    def __init__(self, **kwargs):
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    def __repr__(self):
        return "<MetricPersonRole('%s','%s','%s')>" % (self.person_id, self.type)

orm.mapper(Person, person_table, properties={
    'roles' : orm.relation(Role, backref='person'),
    'metric_role' : orm.relation(MetricPersonRole, backref='person'),
})
orm.mapper(Title, title_table, properties={
    'roles' : orm.relation(Role, backref='title'),
    'data_imdb': orm.relation(DataImdb, uselist=False, backref='title'),
    'data_netflix': orm.relation(DataNetflix, uselist=False, backref='title'),
    'aka_titles': orm.relation(AkaTitle, backref='title'),
})
orm.mapper(Role, role_table)
orm.mapper(DataImdb, data_imdb_table)
orm.mapper(DataNetflix, data_netflix_table)
orm.mapper(AkaTitle, aka_title_table)
orm.mapper(MetricTitle, metric_title_table)
orm.mapper(MetricPersonRole, metric_person_role_table)
