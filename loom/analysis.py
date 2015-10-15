import six
from datetime import datetime, date
from collections import Mapping, Iterable

from jsonmapping.transforms import transliterate

IGNORE_FIELDS = ['$schema', '$sources', '$latin', '$text', '$attrcount',
                 '$linkcount', 'id']


def latinize(text):
    """ Transliterate text to latin. """
    if text is None or not len(text):
        return text
    return transliterate(text).lower()


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
