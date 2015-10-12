from sqlalchemy.schema import Column
from sqlalchemy.types import Unicode, BigInteger

from loom.db.manager import TableManager


def get_properties_manager(config):
    columns = (Column('id', BigInteger, primary_key=True),
               Column('subject', Unicode(1024)),
               Column('predicate', Unicode(255)),
               Column('object', Unicode()),
               Column('type', Unicode(32)),
               Column('source', Unicode(255)))
    indexes = [('subject', ), ('source',)]
    unique = ('subject', 'predicate', 'object', 'source')
    return TableManager(config, 'property', columns, indexes, unique)


def get_entities_manager(config):
    columns = (Column('id', BigInteger, primary_key=True),
               Column('subject', Unicode(1024)),
               Column('schema', Unicode(1024)),
               Column('source', Unicode(255)))
    indexes = [('schema', 'source'), ('schema',), ('source',)]
    unique = ('subject', 'source')
    return TableManager(config, 'entity', columns, indexes, unique)
