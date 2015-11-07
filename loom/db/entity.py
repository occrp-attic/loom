from sqlalchemy import Column, Index, Unicode, Integer, DateTime, func

from loom.db.util import Base, BigIntegerType


class Entity(Base):
    """ Type declarations for type declarations. """
    __tablename__ = 'entity'
    __table_args__ = (
        Index('ix_entity_subject', 'subject'),
        Index('ix_entity_schema', 'schema'),
        Index('ix_entity_source_id', 'source_id')
    )

    id = Column(BigIntegerType, primary_key=True)
    subject = Column(Unicode(1024))
    schema = Column(Unicode(255))
    source_id = Column(Integer, nullable=True)
    collection_id = Column(Integer, nullable=True)
    author = Column(Unicode(512), nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=True)

    def __repr__(self):
        return '<Entity(%r,%r)>' % (self.subject, self.schema)
