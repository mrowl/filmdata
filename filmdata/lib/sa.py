"""
Some SQLAlchemy helpers for use in filmdata.
Includes a special enum type and a generic init function for the models.
"""

import sqlalchemy as sa
from filmdata.lib.dotdict import dotdict

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

class DynamicTables(object):

    def __init__(self, prefix, schemas, properties=None, common_cols=None):
        self.prefix = prefix
        self.schemas = schemas
        self.properties = properties
        self.common_cols = common_cols
        self.tables = {}
        self.classes = dotdict()
        self.build()

    def build():
        for name, schema in self.schemas:
            table_name = '_'.join((prefix, name))
            cols = [
                sa.Column('_'.join((table_name, 'id')),
                          sa.types.Integer, primary_key=True),
            ]
            if self.common_cols is not None:
                cols.extend(self.common_cols())
            cols.extend(self._get_schema_cols(schema))
            cols.extend(self._get_timestamp_cols())

            params = [table_name, meta.metadata]
            params.extend(cols)

            self.tables[name] = sa.Table(*params)

            class_name = ''.join((prefix.capitalize(), name.capitalize()))
            self.classes[name] = type(class_name, (object,),
                                      {'__init__' : table_class_init})

            # map the dynamic table to the class
            orm.mapper(self.classes[name], self.tables[name])
            
            if self.properties:
                self._add_properties(schema.keys(), table_name, name)

    def _add_properties(self, col_names, table_name, name):
        for col in col_names:
            foreign_name = col_name[:-3]
            if col_name[-3:] == '_id' and foreign_name in self.properties:
                relation = orm.relation(self.classes[name],
                                        uselist=False,
                                        backref=foreign_name)
                self.properties[foregin_name][table_name] = relation

    def _get_schema_cols(self, schema):
        cols = []
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
        return cols

    def _get_timestamp_cols(self):
        return [
            sa.Column("created", sa.types.DateTime(), nullable=False,
                      default=datetime.datetime.now),
            sa.Column("modified", sa.types.DateTime(), nullable=False,
                      default=datetime.datetime.now,
                      onupdate=datetime.datetime.now),
        ]
