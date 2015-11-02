from sqlalchemy import Column, Unicode, Integer, ForeignKey, DateTime, func
from sqlalchemy.orm import relationship

from loom.db.util import Base


class CollectionSubject(Base):
    """ A subject marked part of a collection. """
    __tablename__ = 'collection_subject'

    id = Column(Integer, primary_key=True)
    collection_id = Column(Integer, ForeignKey('collection.id'), index=True)
    subject = Column(Unicode, index=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class Collection(Base):
    """ A collection of entities. """
    __tablename__ = 'collection'

    id = Column(Integer, primary_key=True)
    title = Column(Unicode())
    subjects = relationship("CollectionSubject", backref="collection")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    def __repr__(self):
        return '<Collection(%s)>' % self.title
