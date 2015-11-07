from sqlalchemy import Column, Unicode

from loom.db.util import Base, CommonColumnsMixin, session


class Source(Base, CommonColumnsMixin):
    """ Data source. """
    __tablename__ = 'source'

    slug = Column(Unicode(255))
    title = Column(Unicode())
    url = Column(Unicode())

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
            'id': self.id,
            'slug': self.slug,
            'title': self.title,
            'url': self.url,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    def __repr__(self):
        return '<Source(%r)>' % self.slug
