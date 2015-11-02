from sqlalchemy.schema import Column, Index
from sqlalchemy.types import Unicode, BigInteger, Integer, DateTime
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.ext.declarative import declarative_base, declared_attr

Base = declarative_base()
BigIntegerType = BigInteger()
BigIntegerType = BigIntegerType.with_variant(postgresql.BIGINT(), 'postgresql')
BigIntegerType = BigIntegerType.with_variant(sqlite.INTEGER(), 'sqlite')
_sessionmaker = sessionmaker()
session = scoped_session(_sessionmaker)


class Property(Base):
    """ Main statement table. """
    __tablename__ = 'property'

    id = Column(BigIntegerType, primary_key=True)
    subject = Column(Unicode(1024))
    predicate = Column(Unicode(255))
    object = Column(Unicode())
    type = Column(Unicode(32))
    source = Column(Unicode(255))
    # created = Column(DateTime, default=func.now, nullable=True)

    @declared_attr
    def __table_args__(cls):
        return (Index('property_idx_subject', 'subject',
                      postgresql_concurrently=True),
                Index('property_idx_source', 'source',
                      postgresql_concurrently=True),)

    def __repr__(self):
        return '<Entity(%s,%s,%s)>' % (self.subject, self.predicate,
                                       self.object)


class Entity(Base):
    """ Type declarations for type declarations. """
    __tablename__ = 'entity'

    id = Column(BigIntegerType, primary_key=True)
    subject = Column(Unicode(1024))
    schema = Column(Unicode(255))
    source = Column(Unicode(255))
    # created = Column(DateTime, default=func.now, nullable=True)

    @declared_attr
    def __table_args__(cls):
        return (Index('entity_idx_subject', 'subject',
                      postgresql_concurrently=True),
                Index('entity_idx_schema', 'schema',
                      postgresql_concurrently=True),
                Index('entity_idx_source', 'source',
                      postgresql_concurrently=True),)

    def __repr__(self):
        return '<Entity(%s,%s)>' % (self.subject, self.schema)


class Source(Base):
    """ Data source. """
    __tablename__ = 'source'

    id = Column(Integer, primary_key=True)
    slug = Column(Unicode(255))
    title = Column(Unicode())
    url = Column(Unicode())
    # created = Column(DateTime, default=func.now, nullable=True)
    # updated = Column(DateTime, onupdate=func.now, nullable=True)

    @classmethod
    def ensure(cls, data):
        source = session.query(cls).filter_by(slug=data['slug']).first()
        if source is None:
            source = Source()
            source.slug = data.get('slug')
        source.title = data.get('title')
        source.url = data.get('url')
        session.add(source)
        session.commit()
        return source

    def to_dict(self):
        return {
            'slug': self.slug,
            'title': self.title,
            'url': self.url
        }

    def __repr__(self):
        return '<Source(%s)>' % self.slug
