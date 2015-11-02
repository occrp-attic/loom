from loom.db.model import session, Source, Entity, Property, Base  # noqa
from loom.db.manager import TableManager
from loom.db.entities import EntityManager  # noqa


def get_properties_manager(config):
    return TableManager(config, Property)


def get_types_manager(config):
    return TableManager(config, Entity)
