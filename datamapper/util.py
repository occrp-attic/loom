import six
import os
from hashlib import sha1
from collections import MutableMapping, Mapping


class DataMapperException(Exception):
    pass


class SpecException(DataMapperException):
    pass


class ConfigException(DataMapperException):
    pass


class EnvMapping(MutableMapping):

    def __init__(self, data):
        self.data = data

    def __getitem__(self, name):
        value = self.data.get(name)
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


def make_key(data):
    def _stringify(obj):
        if isinstance(obj, dict):
            return '::'.join([_stringify(o) for o in obj.items()])
        elif isinstance(obj, (list, tuple, set)):
            return '||'.join([_stringify(o) for o in sorted(obj)])
        else:
            return unicode(obj).encode('utf-8')

    return sha1(_stringify(data)).hexdigest()
