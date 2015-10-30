from sqlalchemy.schema import Column
from sqlalchemy.types import Unicode, BigInteger, Integer
from sqlalchemy.dialects import postgresql, sqlite

from loom.db.manager import TableManager
from loom.db.entities import EntityManager  # noqa

BigIntegerType = BigInteger()
BigIntegerType = BigIntegerType.with_variant(postgresql.BIGINT(), 'postgresql')
BigIntegerType = BigIntegerType.with_variant(sqlite.INTEGER(), 'sqlite')


def get_properties_manager(config):
    columns = (Column('id', BigIntegerType, primary_key=True),
               Column('subject', Unicode(1024)),
               Column('predicate', Unicode(255)),
               Column('object', Unicode()),
               Column('type', Unicode(32)),
               Column('source', Unicode(255)))
    indexes = [('subject', ), ('source',)]
    unique = ('subject', 'predicate', 'object', 'source')
    return TableManager(config, 'property', columns, indexes, unique)


def get_types_manager(config):
    columns = (Column('id', BigIntegerType, primary_key=True),
               Column('subject', Unicode(1024)),
               Column('schema', Unicode(1024)),
               Column('source', Unicode(255)))
    indexes = [('schema', 'source'), ('schema',), ('source',), ('subject',)]
    unique = ('subject', 'source')
    return TableManager(config, 'entity', columns, indexes, unique)


def get_sources_manager(config):
    columns = (Column('id', Integer, primary_key=True),
               Column('slug', Unicode(1024)),
               Column('title', Unicode()),
               Column('url', Unicode()))
    indexes = [('slug',)]
    unique = ('slug',)
    return TableManager(config, 'source', columns, indexes, unique)
