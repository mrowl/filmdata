"""
Some SQLAlchemy helpers for use in filmdata.
Includes a special enum type and a generic init function for the models.
"""

import datetime
import sqlalchemy as sa
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

    def __init__(self, type, name, schema, meta, seed_cols=None):
        self.type = type
        self.name = name
        self.schema = schema
        self.seed_cols = seed_cols
        self.meta = meta

        self.table_name = '_'.join((type, name))
        self.table = None
        self.model = None
        self.build()

    def build(self):
        cols = [
            sa.Column('_'.join((self.table_name, 'id')),
                      sa.types.Integer, primary_key=True),
        ]
        if self.seed_cols is not None:
            cols.extend(self.seed_cols())
        cols.extend(self._get_schema_cols())
        cols.extend(self._get_timestamp_cols())

        params = [self.table_name, self.meta.metadata] + cols

        self.table = sa.Table(*params)

        class_name = ''.join((self.type.capitalize(), self.name.capitalize()))
        self.model = type(class_name, (object,),
                          {'__init__' : table_class_init})

        # map the dynamic table to the class
        sa.orm.mapper(self.model, self.table)

    def get_relation(self, name):
        if '_'.join((name, 'id')) in self.schema:
            return sa.orm.relation(self.model, uselist=False, backref=name)
        return None

    def _get_col(self, name, type):
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

    def _get_schema_cols(self):
        return [ self._get_col(n, t) for n, t in self.schema.iteritems() ]

    def _get_timestamp_cols(self):
        return [
            sa.Column("created", sa.types.DateTime(), nullable=False,
                      default=datetime.datetime.now),
            sa.Column("modified", sa.types.DateTime(), nullable=False,
                      default=datetime.datetime.now,
                      onupdate=datetime.datetime.now),
        ]
