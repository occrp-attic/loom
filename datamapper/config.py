import os
import yaml

from sqlalchemy import create_engine
from jsonschema import RefResolver

from datamapper.util import ConfigException


class Config(object):
    """ Parsing a configuration file. This specifies the database connection
    and the settings for each data sink. """

    def __init__(self, data, path=None):
        self.data = data or {}
        self.path = path

    @classmethod
    def from_path(cls, path):
        with open(path, 'r') as fh:
            return cls(yaml.load(fh), path=path)

    @property
    def engine(self):
        database = self.data.get('database')
        if database is None:
            raise ConfigException("No database URI configued!")
        return create_engine(database)

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
