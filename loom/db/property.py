from sqlalchemy import Column, Index, DateTime, Unicode, func
from sqlalchemy.ext.declarative import declared_attr

from loom.db.util import Base, BigIntegerType


class Property(Base):
    """ Main statement table. """
    __tablename__ = 'property'

    id = Column(BigIntegerType, primary_key=True)
    subject = Column(Unicode(1024))
    predicate = Column(Unicode(255))
    object = Column(Unicode())
    type = Column(Unicode(32))
    source = Column(Unicode(255))
    created_at = Column(DateTime, default=func.now(), nullable=True)

    @declared_attr
    def __table_args__(cls):
        return (Index('property_idx_subject', 'subject',
                      postgresql_concurrently=True),
                Index('property_idx_source', 'source',
                      postgresql_concurrently=True),)

    def __repr__(self):
        return '<Entity(%s,%s,%s)>' % (self.subject, self.predicate,
                                       self.object)
