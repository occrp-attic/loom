from sqlalchemy import BigInteger, Integer, Column, DateTime, func
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


class CommonColumnsMixin():
    """ Some common attributes for tables. """
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
