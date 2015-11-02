from sqlalchemy.types import BigInteger
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
BigIntegerType = BigInteger()
BigIntegerType = BigIntegerType.with_variant(postgresql.BIGINT(), 'postgresql')
BigIntegerType = BigIntegerType.with_variant(sqlite.INTEGER(), 'sqlite')
_sessionmaker = sessionmaker()
session = scoped_session(_sessionmaker)
