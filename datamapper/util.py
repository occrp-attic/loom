from hashlib import sha1


class DataMapperException(Exception):
    pass

class SpecException(DataMapperException):
    pass

class ConfigException(DataMapperException):
    pass


def make_key(data):
    def _stringify(obj):
        if isinstance(obj, dict):
            return '::'.join([_stringify(o) for o in obj.items()])
        elif isinstance(obj, (list, tuple, set)):
            return '||'.join([_stringify(o) for o in sorted(obj)])
        else:
            return unicode(obj).encode('utf-8')

    return sha1(_stringify(data)).hexdigest()
