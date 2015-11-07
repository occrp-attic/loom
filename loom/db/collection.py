from sqlalchemy import Column, Unicode, Integer, ForeignKey
from sqlalchemy.orm import relationship

from loom.db.util import Base, CommonColumnsMixin


class CollectionSubject(Base, CommonColumnsMixin):
    """ A subject marked part of a collection. """
    __tablename__ = 'collection_subject'

    collection_id = Column(Integer, ForeignKey('collection.id'), index=True)
    subject = Column(Unicode, index=True)

    def __init__(self, collection, subject):
        self.collection = collection
        self.subject = subject


class Collection(Base, CommonColumnsMixin):
    """ A collection of entities. """
    __tablename__ = 'collection'

    title = Column(Unicode())
    subjects = relationship(CollectionSubject, backref="collection")

    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title,
            'subjects': [s.subject for s in self.subjects],
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    def __repr__(self):
        return '<Collection(%r)>' % self.title
