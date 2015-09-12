

class Source(object):
    """ Identifying the source of a record. """

    def __init__(self, data):
        self.slug = data.get('slug')
        self.title = data.get('title')
        self.url = data.get('url')
        self.data = data

    def to_dict(self):
        return self.data
