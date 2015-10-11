from loom.db.manager import Manager


def get_properties_manager(config):
    indexes = [('subject', ), ('source',)]
    unique = ('subject', 'predicate', 'object', 'source')
    return Manager(config, 'property', indexes, unique)


def get_entities_manager(config):
    indexes = [('schema', 'source'), ('schema',), ('source',)]
    unique = ('subject', 'schema', 'source')
    return Manager(config, 'entity', indexes, unique)
