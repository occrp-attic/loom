import six
import os
import string
from uuid import uuid4
from collections import MutableMapping, Mapping

ALPHABET = string.letters + string.digits


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

    def get(self, name, default=None):
        value = self.data.get(name)
        if value is None:
            return default
        if isinstance(value, six.string_types) and not len(value.strip()):
            return default
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


def make_id():
    uuid = uuid4().int
    chars = [u'urn:']
    while uuid:
        uuid, digit = divmod(uuid, len(ALPHABET))
        chars.append(ALPHABET[digit])
    return u''.join(chars)
