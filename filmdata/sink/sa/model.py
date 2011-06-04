"""The application's model objects"""
#from sqlalchemy.dialects.postgresql import UUID

import datetime

import sqlalchemy as sa
from sqlalchemy import orm, event, DDL
from sqlalchemy.ext.associationproxy import association_proxy

import filmdata.source
import filmdata.metric
from filmdata import config
from filmdata.sink.sa import meta
from filmdata.lib.sa import EnumIntType, DynamicModels

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
    sa.UniqueConstraint("name", "year", "type", name="title_info_unq"),
    )
title_lower_index = DDL(
    'create index title_name_lower_idx on title ((lower(name)))')
title_trgm_index = DDL('create index title_name_trgm_idx'
                       'on title using gin (name gin_trgm_ops)')
event.listen(title_table, 'after_create',
             title_lower_index.execute_if(dialect='postgresql'))
event.listen(title_table, 'after_create',
             title_trgm_index.execute_if(dialect='postgresql'))

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
    sa.UniqueConstraint("name", "year", "region", name="aka_title_info_unq"),
    )
aka_title_lower_index = DDL(
    'create index aka_title_name_lower_idx on aka_title ((lower(name)))')
aka_title_trgm_index = DDL('create index aka_title_name_trgm_idx'
                           'on aka_title using gin (name gin_trgm_ops)')
event.listen(aka_title_table, 'after_create',
             aka_title_lower_index.execute_if(dialect='postgresql'))
event.listen(aka_title_table, 'after_create',
             aka_title_trgm_index.execute_if(dialect='postgresql'))

role_table = sa.Table("role", meta.metadata,
    sa.Column("role_id", sa.types.Integer, primary_key=True),
    sa.Column("person_id", sa.types.Integer,
              sa.ForeignKey("person.person_id")),
    sa.Column("title_id", sa.types.Integer, sa.ForeignKey("title.title_id")),
    sa.Column("type", EnumIntType(config.ROLE_TYPES), nullable=False),
    sa.Column("character", sa.types.Unicode(1023), nullable=True),
    sa.Column("billing", sa.types.SmallInteger, nullable=True, default=0),
    sa.Column("created", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now),
    sa.Column("modified", sa.types.DateTime(), nullable=False,
              default=datetime.datetime.now, onupdate=datetime.datetime.now),
    sa.UniqueConstraint("person_id", "title_id", "type", 
                        name="person_title_link_unq"),
    )

class Person(object):
    """
    SQLAlchemy Model for a person, e.g. actor.
    """

    titles = association_proxy('roles', 'title')

    def __init__(self, name=None):
        """Create a new person object"""
        self.name = name

    def __repr__(self):
        """String representation of this person"""
        return "<Person('%s')>" % (self.name)

    def as_dict(self):
        """Returns this object as a dict mapping attribute names to values"""
        excludes = ['titles', 'roles', 'created', 'modified']
        d = {}
        for p in orm.util.object_mapper(self).iterate_properties:
            if p.key not in excludes:
                d[p.key] = getattr(self, p.key)
        return d

class Title(object):
    """
    SQLAlchemy Model for a title, e.g. a film.
    """

    people = association_proxy('roles', 'person')

    def __init__(self, name=None, year=None, type=None):
        """Create a new title object"""
        self.name = name
        self.year = year
        self.type = type

    def __repr__(self):
        """String representation of this title"""
        return "<Title('%s','%s', '%s', '%s', '%s')>" % (self.name,
                                                          self.year,
                                                          self.type,
                                                         )

    def as_dict(self, include=None):
        """Returns this object as a dict mapping attribute names to values"""
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
    """
    SQLAlchemy Model for an alternate name of a title.
    """

    def __init__(self, title_id=None, name=None, year=None, region=None):
        """Create a new akatitle"""
        self.name = name
        self.year = year
        self.region = region
        self.title_id = title_id

    def __repr__(self):
        """String representation of this akatitle"""
        return "<AkaTitle('%s','%s', '%s', '%s')>" % (self.name,
                                                      self.year,
                                                      self.region,
                                                      self.title_id
                                                     )

class Role(object):
    """
    SQLAlchemy model of a role (i.e. the title and person who worked on it)
    """

    def __init__(self, type=None, character=None, billing=None,
                 person_id=None, title_id=None):
        """Create a new role"""
        self.type = type
        self.character = character
        self.billing = billing
        self.person_id = person_id
        self.title_id = title_id

    def __repr__(self):
        """String representation of this role"""
        return "<Role('%s')>" % (self.type, self.character, self.billing,
                                 self.person_id, self.title_id)

# metric holds all the metric plugin models and they can be accessed
# via metric.<plugin_name> or metric[<plugin_name>]. Likewise for source.
metric = DynamicModels(dict([ (n, s.schema) for n, s in
                              filmdata.metric.manager.iter() ]),
                       meta, 'metric')
source = DynamicModels(dict([ (n, s.schema) for n, s in
                              filmdata.source.manager.iter() ]),
                       meta, 'source')

# Used for culling titles based on number of votes so
# really rare titles aren't included in the metrics.
culler = source[config.core.master_data]

# properties for the foreign relations (see below)
properties = {'title' : {}, 'person' : {}}
for pkg in (metric, source):
    pkg_properties = pkg.get_properties(properties.keys())
    for property_key in properties.keys():
        for k, v in pkg_properties[property_key].iteritems():
            properties[property_key][k] = v


properties['title']['roles'] = orm.relation(Role, backref='title') 
properties['title']['aka_titles'] = orm.relation(AkaTitle, backref='title')
orm.mapper(Title, title_table, properties=properties['title'])

properties['person']['roles'] = orm.relation(Role, backref='person')
orm.mapper(Person, person_table, properties=properties['person'])

orm.mapper(Role, role_table)
orm.mapper(AkaTitle, aka_title_table)
