import os
import yaml

from sqlalchemy import create_engine
from jsonschema import RefResolver

from datamapper.util import ConfigException
from datamapper.sinks import Sink


class Config(dict):
    """ Parsing a configuration file. This specifies the database connection
    and the settings for each data sink. """

    def __init__(self, data, path=None):
        self.update(data)
        self.path = path

    @classmethod
    def from_path(cls, path):
        with open(path, 'r') as fh:
            return cls(yaml.load(fh), path=path)

    @property
    def engine(self):
        if not hasattr(self, '_engine'):
            database = self.get('database')
            if database is None:
                raise ConfigException("No database URI configued!")
            self._engine = create_engine(database)
        return self._engine

    @property
    def base_uri(self):
        if self.path is None:
            config_uri = 'file:///tmp'
        else:
            config_uri = 'file://' + os.path.abspath(self.path)
        return self.data.get('base_uri', config_uri)

    @property
    def resolver(self):
        if not hasattr(self, '_resolver'):
            self._resolver = RefResolver(self.base_uri, self.base_uri)
        return self._resolver

    @property
    def sink(self):
        cls = Sink.by_name(self.get('sink'))
        if cls is None:
            raise ConfigException("No such sink type: %r" % self.get('sink'))
        return cls
