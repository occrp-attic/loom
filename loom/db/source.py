from sqlalchemy import Column, Unicode, Integer, DateTime
from sqlalchemy import func

from loom.db.util import Base, session


class Source(Base):
    """ Data source. """
    __tablename__ = 'source'

    id = Column(Integer, primary_key=True)
    slug = Column(Unicode(255))
    title = Column(Unicode())
    url = Column(Unicode())
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

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
            'url': self.url,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    def __repr__(self):
        return '<Source(%s)>' % self.slug
