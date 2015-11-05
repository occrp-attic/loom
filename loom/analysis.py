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


def extract_text(data):
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
        return [data]
    if isinstance(data, Iterable):
        values = []
        for d in data:
            values.extend(extract_text(d))
        return values
