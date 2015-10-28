import six
import os
import yaml
from collections import MutableMapping, Mapping


class LoomException(Exception):
    pass


class SpecException(LoomException):
    pass


class ConfigException(LoomException):
    pass


def load_config(config_file):
    """ Loads the configuration file and recursively imports any base
    configurations indicated in the configuration. """
    with open(config_file, 'r') as fh:
        return yaml.load(fh)


class EnvMapping(MutableMapping):

    def __init__(self, data):
        self.data = data

    def __getitem__(self, name):
        return self.get(name)

    def get(self, name, default=None, raw=False):
        value = self.data.get(name)
        if value is None or (isinstance(value, six.string_types)
                             and not len(value.strip())):
            value = default
        if not raw:
            if isinstance(value, Mapping):
                return EnvMapping(value)
            if isinstance(value, six.string_types):
                return os.path.expandvars(value)
        return value

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __contains__(self, name):
        return name in self.data

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)
