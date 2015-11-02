from sqlalchemy.schema import Column
from sqlalchemy.types import Unicode, Integer

from loom.db.util import Base


class Collection(Base):
    """ A collection of entities. """
    __tablename__ = 'collection'

    id = Column(Integer, primary_key=True)
    title = Column(Unicode())

    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title
        }

    def __repr__(self):
        return '<Collection(%s)>' % self.title
