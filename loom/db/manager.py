import logging

import pymongo

from loom.db.writer import Writer

log = logging.getLogger(__name__)


class Manager(object):
    """ The manager controls writing and reading data from MongoDB
    collections. """

    def __init__(self, config, name, indexes, unique):
        self.config = config
        self.name = name
        self.indexes = indexes
        self.unique = unique

    @property
    def collection(self):
        """ Generate an appropriate collecton. """
        if not hasattr(self, '_collection'):
            self._collection = self.config.mongo[self.name]
            for columns in self.indexes:
                log.info("Adding DB index %r: %r", self.name, columns)
                desc = [(c, pymongo.ASCENDING) for c in columns]
                self._collection.create_index(desc, background=True)
        return self._collection

    def writer(self):
        return Writer(self)

    def __repr__(self):
        return "<TableManager(%r)>" % (self.name)
