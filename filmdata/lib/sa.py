"""
Some SQLAlchemy helpers for use in filmdata.
Includes a special enum type and a generic init function for the models.
"""

import datetime
import sqlalchemy as sa
from UserDict import IterableUserDict
from filmdata.lib.dotdict import dotdict
from filmdata import config

def table_class_init(self, **kwargs):
    """
    A basic function which takes in keyword arguments and uses them to set
    the attributes of the instance. Useful for setting the __init__ method
    of a dynamically created class.
    """
    for k,v in kwargs.iteritems():
        setattr(self, k, v)

class EnumIntType(sa.types.TypeDecorator):
    """
    A new enum type for SQLAlchemy which uses the integer type in the 
    DB for storage. It uses a list to maintain the relationship between
    the integer (backend) and the string (frontend).
    Attributes:
        values - the list of possible values for this column
        impl - a SQLA thing which denotes what type to use underneath
            (SmallInteger in this case)
    Example:
        #in your table declaration
        animal_table = sa.Table("title", meta,
            sa.Column("name", sa.types.Unicode(31), nullable=False),
            sa.Column("type", EnumIntType(("dog", "cat", "narwhal")), nullable=False))

        #in a query
        session.query(Animal).filter(Animal.type == "cat").all()
    """
    impl = sa.types.SmallInteger

    values = None

    def __init__(self, values=None):
        """
        Create a new enum type.
        Arguments:
            values - the list of enumerated values
        """
        sa.types.TypeDecorator.__init__(self)
        self.values = values

    def process_bind_param(self, value, dialect):
        """
        Runs when the column is trying to be set.  Gets passed one of the
        strings in the enumerated list and looks up the index of that
        value in the list.  That integer will then get passed down to
        the lower levels for storage in the db.
        Arguments:
            value - the string value for the column
            dialect - unused sqla thing
        """
        return None if value == None else self.values.index(value)

    def process_result_value(self, value, dialect):
        """
        Getting the result for the column.  This gets passed the raw value
        (i.e. an integer) from the db and looks it up in the enum list
        to find the corresponding human readable string that the
        integer represents.
        Arguments:
            value - the string value for the column
            dialect - unused sqla thing
        """
        return None if value == None else self.values[value]

class DynamicModel(object):
    """
    A dynamic ORM model for SQLAlchemy.
    Attributes:
        name - the name of the model
        schema - the simple schema of the model
        seed_cols - a function to use to generate any additional columns
        meta - the sqlalchemy meta object
        table_name - the name of the table underlying this model
        table - the sqla table object for this model
        model - the sqla model object
    """

    __init__ = table_class_init

    @classmethod
    def get_relation(this, name):
        if '_'.join((name, 'id')) in this.tbl_obj.columns:
            return sa.orm.relation(this, uselist=False, backref=name)
        return None

class DynamicModelFactory:

    """
    Create a new DynamicModel instance.
    Arguments:
        name - a tuple containing the pieces of the name of the table
        schema - the table schema
        meta - the meta object from sqlalchemy
        seed_cols - a function that returns some sqla columns to use in
            the table (given priority over the schema columns)
    """
    @classmethod
    def build(this, model_name, schema, meta, seed_cols=None):
        tbl_name = '_'.join(model_name)
        cols = [
            sa.Column('_'.join((tbl_name, 'id')),
                      sa.types.Integer, primary_key=True),
        ]
        if seed_cols is not None:
            cols.extend(seed_cols())
        cols.extend(this._get_schema_cols(schema))
        cols.extend(this._get_timestamp_cols())

        params = [tbl_name, meta.metadata] + cols

        tbl_obj = sa.Table(*params)

        class_name = ''.join([ n.capitalize() for n in model_name ])
        model_class = type(class_name, (DynamicModel,),
                           { '__init__' : table_class_init,
                             'tbl_name' : tbl_name,
                             'tbl_obj' : tbl_obj })

        # map the dynamic table to the class
        sa.orm.mapper(model_class, tbl_obj)
        return model_class

    @classmethod
    def _get_col(this, name, type):
        if type == 'id' and name in ('title_id', 'person_id'):
            tbl_name = name.partition('_')[0]
            return sa.Column(name, sa.types.Integer,
                             sa.ForeignKey('.'.join((tbl_name, name))))
        elif name == 'role_type' and type == None:
            return sa.Column(name, EnumIntType(config.ROLE_TYPES),
                             nullable=False)
        elif name == 'rating' and type is None:
            return sa.Column(name, sa.types.Numeric(asdecimal=True, scale=1))
        elif type == 'decimal':
            return sa.Column(name, sa.types.Numeric(asdecimal=True))
        return sa.Column(name, sa.types.Integer)

    @classmethod
    def _get_schema_cols(this, schema):
        return [ this._get_col(n, t) for n, t in schema.iteritems() ]

    @classmethod
    def _get_timestamp_cols(this):
        return [
            sa.Column("created", sa.types.DateTime(), nullable=False,
                      default=datetime.datetime.now),
            sa.Column("modified", sa.types.DateTime(), nullable=False,
                      default=datetime.datetime.now,
                      onupdate=datetime.datetime.now),
        ]

class DynamicModels(dotdict, IterableUserDict): 
    """
    A class for holding a dictionary of DynamicModel objects.
    """

    def __init__(self, schemas, meta, name=None, seed_cols=None):
        IterableUserDict.__init__(self)
        self._build(schemas, meta, name, seed_cols)

    def get_sa_cols(self, col_names):
        return [ ('_'.join((model_name, col_name)),
                  getattr(model_class, col_name))
                 for model_name, model_class in self.iteritems()
                 for col_name in col_names
                 if hasattr(model_class, col_name) ]

    def get_tables(self):
        return [ model.tbl_obj for model in self.values() ]
    
    def get_properties(self, relation_names):
        properties = {}
        for relation_name in relation_names:
            properties[relation_name] = {}
            for model in self.values():
                relation = model.get_relation(relation_name)
                if relation is not None:
                    properties[relation_name][model.tbl_name] = relation
        return properties

    def _build(self, schemas, meta, name, seed_cols):
        name_seq = name.split('_') if name else tuple()
        for model_name, schema_def in schemas.iteritems():
            dyna_model_name = name_seq + model_name.split('_')
            dyna_model = DynamicModelFactory.build(dyna_model_name,
                                                   schema_def,
                                                   meta,
                                                   seed_cols)
            self[model_name] = dyna_model
