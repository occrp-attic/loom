from sqlalchemy import Column, Index, Unicode, DateTime, func
from sqlalchemy.ext.declarative import declared_attr

from loom.db.util import Base, BigIntegerType


class Entity(Base):
    """ Type declarations for type declarations. """
    __tablename__ = 'entity'

    id = Column(BigIntegerType, primary_key=True)
    subject = Column(Unicode(1024))
    schema = Column(Unicode(255))
    source = Column(Unicode(255))
    created_at = Column(DateTime, default=func.now(), nullable=True)

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
