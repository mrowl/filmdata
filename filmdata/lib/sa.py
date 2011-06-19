"""
Some SQLAlchemy helpers for use in filmdata.
Includes a special enum type and a generic init function for the models.
"""

import datetime, re
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
            sa.Column("type", EnumIntType(("dog", "cat", "narwhal")),
                      nullable=False))

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
        tbl_name - the name of the table underlying this model
        tbl_obj - the sqla table object for this model
    """

    tbl_name = None
    tbl_obj = None

    # Use the generic constructor for a table class
    __init__ = table_class_init

    @classmethod
    def get_relation(cls, name):
        """
        Return the SQLAlchemy relation object for this table, given the name
        of the related table. Useful for setting up mappings with the orm.
        Arguments:
            name - the name of the related table
        Returns a SQLAlchemy relation from the orm library.
        """
        if '_'.join((name, 'id')) in cls.tbl_obj.columns:
            return sa.orm.relation(cls, uselist=False, backref=name)
        return None

class DynamicModelFactory:
    """
    A convenience factory for creating dynamic sqlalchemy models.
    """

    @classmethod
    def build(cls, model_name, schema, meta, seed_cols=None):
        """
        Create a new model class.
        Arguments:
            model_name - a tuple containing the pieces of the name of the table
            schema - the table schema
            meta - the meta object from sqlalchemy
            seed_cols - a function that returns some sqla columns to use in
                the table (given priority over the schema columns)
        Returns the new model class mapped to the proper table.
        """
        tbl_name = '_'.join(model_name)
        cols = [
            sa.Column('_'.join((tbl_name, 'id')),
                      sa.types.Integer, primary_key=True),
        ]
        if seed_cols is not None:
            cols.extend(seed_cols())
        cols.extend(cls._get_schema_cols(schema))
        cols.extend(cls._get_timestamp_cols())

        params = [tbl_name, meta.metadata] + cols

        tbl_obj = sa.Table(*params)

        class_name = ''.join([ n.capitalize() for n in model_name ])

        # create a new class (a sqla model) which extends the DynamicModel
        # (above) and sets the tbl_name and tbl_obj class/static vars
        model_class = type(class_name, (DynamicModel,),
                           { 'tbl_name' : tbl_name,
                             'tbl_obj' : tbl_obj })

        # map the table to the new class via the orm
        sa.orm.mapper(model_class, tbl_obj)
        return model_class

    @classmethod
    def _get_col(cls, name, type):
        """
        Get a proper sqlalchemy object based on a name and data type.
        Translates from the rather simple proprietary fimdata column
        definition to the sqla column objects.
        Arguments:
            name - the name of the column
            type - the data type of the column
        Returns a sqlalchemy column object (probably to go into a table def).
        """
        col_args = [ name ]
        col_kwargs = {}
        if type == 'id' and name in ('title_id', 'person_id'):
            tbl_name = name.partition('_')[0]
            col_args += [ sa.types.Integer,
                          sa.ForeignKey('.'.join((tbl_name, name))) ]
        elif name == 'role_type' and type == None:
            col_args += [ EnumIntType(config.ROLE_TYPES) ]
            col_kwargs['nullable'] = False
        elif name == 'rating' and type is None:
            col_args += [ sa.types.Numeric(asdecimal=True, scale=1) ]
        elif type == 'decimal':
            col_args += [ sa.types.Numeric(asdecimal=True) ]
        elif type[:7] == 'varchar':
            match = re.match('varchar\(([0-9]+)\)', type)
            col_len = int(match.group(1)) if match else 31
            col_args += [ sa.types.Unicode(col_len) ]
        elif type == 'smallint' or type == 'tinyint':
            col_args += [ sa.types.SmallInteger ]
        else: #should be 'integer'
            col_args += [ sa.types.Integer ]
        if name == 'key':
            col_kwargs['nullable'] = False
            col_kwargs['unique'] = True
        return sa.Column(*col_args, **col_kwargs)

    @classmethod
    def _get_schema_cols(cls, schema):
        """
        Maps the columns defined by the filmdata schema definition dictionary
        to a a list of columns.
        Arguments:
            schema - the dictionary with the schema
        Returns a list of sqlalchemy columns.
        """
        return [ cls._get_col(n, t) for n, t in schema.iteritems() ]
    
    @classmethod
    def _get_timestamp_cols(cls):
        """
        Helper for creating the two timestamp columns that go at the end of
        each table. The column names are "created" and "modified" following the
        convention that pylons uses.
        Returns a list of length two holding new sqlalchemy column objects.
        """
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
        """
        Create a new DynamicModels instance.
        Arguments:
            schemas - a list of all the schemas for the underlying tables
            meta - the meta object for this sqla session
            name - the name for this instance (will be the prefix to the
                table names)
            seed_cols - a lambda function which will generate columns present
                in each of the underlying tables
        """
        IterableUserDict.__init__(self)
        self._build(schemas, meta, name, seed_cols)

    def get_sa_cols(self, col_names):
        """
        Get the column keys (i.e. "<tbl_name>_<col_name>") and objects for
        each of the underlying tables.
        Helps keep querying dynamic, so you don't have to worry so much about
        adding/removing tables.
        e.g.
            # Assume source holds two classes: SourceImdb and SourceNetflix.
            # To get SourceImdb.rating, SourceImdb.votes, SourceNetflix.rating,
            # and SourceNetflix.votes for use in querying you can run this:
            col_names = ('rating', 'votes')
            data_keys, data_cols = zip(*source.get_sa_cols(col_names))

            # then query with
            rows = session.query(*data_cols).all()
            #
            # instead of
            # session.query(SourceImdb.rating, SourceImdb.votes,
            #               SourceNetflix.rating, SourceNetflix.votes).all()
            # 
            # data_keys are nice to have because they hold the
            # <tbl_name>_<col_name>
        Arguments:
            col_names - a sequence of the names of the columns from the
                underlying tables to get
        Returns a zipped list of column keys and column objects.
        """
        return [ ('_'.join((model_name, col_name)),
                  getattr(model_class, col_name))
                 for model_name, model_class in self.iteritems()
                 for col_name in col_names
                 if hasattr(model_class, col_name) ]

    def get_tables(self):
        """
        Get all the table objects that the model classes are mapped to.
        Returns a list of sqla tables.
        """
        return [ model.tbl_obj for model in self.values() ]
    
    def get_properties(self, relation_names):
        """
        Get all the sqlalchemy relation properties for each of the models.
        Arguments:
            relation_names - a sequence of the names of the tables to which our
                models are related
        Returns a dictionary holding the orm relations.
            { 'name of relation' : { 'our model's table name' :
                                     orm_relation_obj } }
        """
        properties = {}
        for relation_name in relation_names:
            properties[relation_name] = {}
            for model in self.values():
                relation = model.get_relation(relation_name)
                if relation is not None:
                    properties[relation_name][model.tbl_name] = relation
        return properties

    def _build(self, schemas, meta, name, seed_cols):
        """
        Do the grunt work of building the dictionary of models.
        Arguments: see __init__
        """
        name_seq = name.split('_') if name else tuple()
        for model_name, schema_def in schemas.iteritems():
            dyna_model_name = name_seq + model_name.split('_')
            dyna_model = DynamicModelFactory.build(dyna_model_name,
                                                   schema_def,
                                                   meta,
                                                   seed_cols)
            self[model_name] = dyna_model
