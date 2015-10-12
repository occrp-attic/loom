import six
import os
import yaml
from uuid import uuid4
from datetime import datetime, date
from collections import MutableMapping, Mapping, Iterable

IGNORE_FIELDS = ['$schema', '$sources', '$latin', '$text', '$attrcount',
                 '$linkcount', 'id']


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
        config = yaml.load(fh)
    if 'base' not in config:
        return config
    base_file = os.path.expandvars(config.pop('base'))
    base_config = load_config(base_file)
    base_config.update(config)
    return base_config


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


def make_id():
    """ Generate a URN uuid. """
    return uuid4().urn


def extract_text(data, sep=' : '):
    """ Get all the instances of text from a given object, recursively. """
    if isinstance(data, Mapping):
        values = []
        for k, v in data.items():
            if k in IGNORE_FIELDS:
                continue
            values.append(v)
        data = values
    if isinstance(data, (date, datetime)):
        data = data.isoformat()
    elif isinstance(data, (int, float)):
        data = six.text_type(data)
    if isinstance(data, six.string_types):
        return data
    if isinstance(data, Iterable):
        text = [extract_text(d, sep=sep) for d in data]
        return sep.join([t for t in text if t is not None])


def count_attrs(data):
    """ Count the number of overall attributes and nested objects which a
    dictionary has. """
    attr_count, link_count = 0, 0
    for field, value in data.items():
        if field in IGNORE_FIELDS:
            continue
        attr_count += 1
        if isinstance(value, dict):
            link_count += 1
        elif isinstance(value, (list, set, tuple)):
            link_count += len(value)
    return attr_count, link_count
