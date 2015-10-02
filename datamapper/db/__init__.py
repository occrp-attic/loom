from sqlalchemy.schema import Column
from sqlalchemy.types import Unicode, BigInteger, DateTime

from datamapper.db.manager import TableManager


def get_properties_manager(meta):
    columns = (Column('id', BigInteger, primary_key=True),
               Column('subject', Unicode(1024)),
               Column('predicate', Unicode(255)),
               Column('object', Unicode()),
               Column('type', Unicode(32)),
               Column('source', Unicode(255)),
               Column('timestamp', DateTime()))
    indexes = [('subject', ), ]
    return TableManager(meta, '_property', columns, indexes)


def get_entities_manager(meta):
    columns = (Column('id', BigInteger, primary_key=True),
               Column('subject', Unicode(1024)),
               Column('schema', Unicode(1024)),
               Column('source', Unicode(255)),
               Column('timestamp', DateTime()))
    indexes = [('schema', 'source'), ('schema',)]
    return TableManager(meta, '_entity', columns, indexes)
