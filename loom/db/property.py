from sqlalchemy import Column, Index, DateTime, Unicode, func

from loom.db.util import Base, BigIntegerType


class Property(Base):
    """ Main statement table. """
    __tablename__ = 'property'
    __table_args__ = (
        Index('ix_property_subject', 'subject'),
        Index('ix_property_source', 'source')
    )

    id = Column(BigIntegerType, primary_key=True)
    subject = Column(Unicode(1024))
    predicate = Column(Unicode(255))
    object = Column(Unicode())
    type = Column(Unicode(32))
    source = Column(Unicode(255))
    created_at = Column(DateTime, default=func.now(), nullable=True)

    def __repr__(self):
        return '<Entity(%s,%s,%s)>' % (self.subject, self.predicate,
                                       self.object)
