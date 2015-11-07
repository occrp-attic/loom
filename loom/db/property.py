from sqlalchemy import Column, Index, DateTime, Integer, Unicode, func

from loom.db.util import Base, BigIntegerType


class Property(Base):
    """ Main statement table. """
    __tablename__ = 'property'
    __table_args__ = (
        Index('ix_property_subject', 'subject'),
        Index('ix_property_source_id', 'source_id')
    )

    id = Column(BigIntegerType, primary_key=True)
    subject = Column(Unicode(1024))
    predicate = Column(Unicode(255))
    object = Column(Unicode())
    type = Column(Unicode(32))
    source_id = Column(Integer, nullable=True)
    collection_id = Column(Integer, nullable=True)
    author = Column(Unicode(512), nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=True)

    def __repr__(self):
        return '<Property(%r,%r,%r)>' % (self.subject, self.predicate,
                                         self.object)
