"""The application's model objects"""
#from sqlalchemy.dialects.postgresql import UUID

import datetime

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.associationproxy import association_proxy

import filmdata.source
import filmdata.metric
from filmdata import config
from filmdata.sinks.sa import meta
from filmdata.lib.dotdict import dotdict
from filmdata.lib.sa import EnumIntType, DynamicModel

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
    sa.Column("type", EnumIntType(config.TITLE_TYPES), nullable=False),
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
    sa.Column("type", EnumIntType(config.ROLE_TYPES), nullable=False),
    sa.Column("character", sa.types.Unicode(1023), nullable=True),
    sa.Column("billing", sa.types.SmallInteger, nullable=True, default=0),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    sa.UniqueConstraint("person_id", "title_id", "type", 
                        name="person_title_link"),
    )

# definitions for building dynamic tables for the plugin packages (metric,
# source) value is a tuple with the schemas and the seeder callback for making 
# columns that are common between all the plugin modules in that package.
plugin_pkgs = {
    'metric' : ([(n, s.schema) for n, s in filmdata.metric.manager.iter()],
                None),
    'source' : ([(n, s.schema) for n, s in filmdata.source.manager.iter()],
                None),
}

# properties for the foreign relations (see bottom of this file)
properties = {'title' : {}, 'person' : {}}

for pkg_name, structures in plugin_pkgs.items():
    schemas, common_cols = structures

    tables = {}
    models = dotdict()
    for name, schema in schemas:
        dyna_model = DynamicModel(pkg_name, name, schema, meta, common_cols)
        tables[name] = dyna_model.table
        models[name] = dyna_model.model

        for relation_name in properties.keys():
            relation = dyna_model.get_relation(relation_name)
            if relation is not None:
                properties[relation_name][dyna_model.table_name] = relation

    if pkg_name == 'metric': 
        metric = models
        metric_tables = tables
    elif pkg_name == 'source': 
        source = models
        source_tables = tables

culler = source[config.core.master_data]

data_tables = []
data_cols = []
data_keys = []
for source_name, source_class in source.iteritems():
    data_tables.append(source_class)
    for data_type in ('rating', 'votes'):
        if hasattr(source_class, data_type):
            data_cols.append(getattr(source_class, data_type))
            data_keys.append((source_name, data_type))

data_dicter = lambda r, o: dict([ ('_'.join(k), r[o + i]) for
                                  i, k in enumerate(data_keys) ])


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
