from datamapper.util import make_key


class Record(object):
    """ A generated record. """

    def __init__(self, source, schema, entity, raw):
        self.source = source
        self.schema = schema
        self.entity = entity
        self.raw = raw

    @property
    def id(self):
        key = self.entity.get('id')
        if key is None:
            key = make_key(self.entity)
        return '%s:%s' % (self.source.slug, key)

    def to_dict(self):
        return {
            'source': self.source.to_dict(),
            'id': self.id,
            'entity': self.entity,
            'schema': self.schema,
            'raw': self.raw
        }
