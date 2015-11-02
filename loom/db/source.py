from sqlalchemy.schema import Column
from sqlalchemy.types import Unicode, Integer

from loom.db.util import Base, session


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
