"""The application's model objects"""
#from sqlalchemy.dialects.postgresql import UUID

import datetime

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.associationproxy import association_proxy

import filmdata.source
import filmdata.metric
from filmdata.sinks.sa import meta
from filmdata.lib.dotdict import dotdict
from filmdata.lib.sa import EnumIntType, table_class_init

TITLE_TYPES = ('film', 'tv')
ROLE_TYPES = ('director', 'actor', 'actress', 'producer', 'writer')

def init_model(engine):
    """Call me before using any of the tables or classes in the model"""
    meta.Session.configure(bind=engine)
    meta.engine = engine

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


properties = {'title' : {}, 'person' : {}}
#common_cols needs to be a function or else it will try to
#apply the same col to multiple tables, illegal
def dyna_tables(prefix, schemas, common_cols=None):
    tables = {}
    classes = dotdict()

    for name, schema in schemas:
        table_name = '_'.join((prefix, name))
        cols = [
            sa.Column('_'.join((table_name, 'id')),
                      sa.types.Integer, primary_key=True),
        ]
        if common_cols is not None:
            cols.extend(common_cols())

        for col_name, col_type in schema.iteritems():
            if col_name == 'person' and col_type == 'id':
                cols.append(sa.Column('person_id', sa.types.Integer,
                                      sa.ForeignKey('person.person_id')))
            elif col_name == 'title' and col_type == 'id':
                cols.append(sa.Column('title_id', sa.types.Integer,
                                      sa.ForeignKey('title.title_id')))
            elif col_name == 'role_type' and col_type == None:
                cols.append(sa.Column('role_type', EnumIntType(ROLE_TYPES),
                                      nullable=False))
            elif col_type == 'decimal':
                cols.append(sa.Column(col_name,
                                      sa.types.Numeric(asdecimal=True)))
            elif col_type == 'integer':
                cols.append(sa.Column(col_name, sa.types.Integer))

        cols.extend([
            sa.Column("created", sa.types.DateTime(), nullable=False,
                      default=datetime.datetime.now),
            sa.Column("modified", sa.types.DateTime(), nullable=False,
                      default=datetime.datetime.now,
                      onupdate=datetime.datetime.now),
        ])

        params = [table_name, meta.metadata]
        params.extend(cols)

        tables[name] = sa.Table(*params)

        class_name = ''.join((prefix.capitalize(), name.capitalize()))
        classes[name] = type(class_name, (object,),
                             {'__init__' : table_class_init})

        # map the dynamic table to the class
        orm.mapper(classes[name], tables[name])

        for col in cols:
            for k in properties.keys():
                if col.name == '_'.join((k, 'id')):
                    properties[k][table_name] = orm.relation(classes[name],
                                                             uselist=False,
                                                             backref=k)

    return tables, classes

source_common_cols = lambda: [
    sa.Column("title_id", sa.types.Integer,
              sa.ForeignKey("title.title_id"), unique=True),
    sa.Column("rating", sa.types.Numeric(asdecimal=True, scale=1))]
source_schemas = [(n, s.schema) for n, s in filmdata.source.manager.iter()]
source_tables, source = dyna_tables('data', source_schemas, source_common_cols)

metric_schemas = [(n, s.schema) for n, s in filmdata.metric.manager.iter()]
metric_tables, metric = dyna_tables('metric', metric_schemas)

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


properties['title']['roles'] = orm.relation(Role, backref='title') 
properties['title']['aka_titles'] = orm.relation(AkaTitle, backref='title')
orm.mapper(Title, title_table, properties=properties['title'])

properties['person']['roles'] = orm.relation(Role, backref='person')
orm.mapper(Person, person_table, properties=properties['person'])

orm.mapper(Role, role_table)
orm.mapper(AkaTitle, aka_title_table)
