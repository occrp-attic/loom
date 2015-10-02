import time
import logging
from datetime import datetime

from datamapper.model import Binding, triplify, TYPE_TYPE
from datamapper.generator import Generator

log = logging.getLogger(__name__)


class Mapper(object):
    """ Map generated records to the data model. """

    def __init__(self, config, model):
        self.config = config
        self.generator = Generator(config, model)

    def records(self, mapping):
        schema = None
        begin = time.time()
        for i, (schema_, data) in enumerate(self.generator.generate(mapping)):
            if schema is None:
                _, schema = self.config.resolver.resolve(schema_)

            binding = Binding(schema, self.config.resolver, data=data)
            for stmt in triplify(binding):
                yield stmt

            if i > 0 and i % 10000 == 0:
                elapsed = time.time() - begin
                per_record = float(elapsed) / float(i)
                speed = per_record * 1000
                log.info("Generating %r: %s records (%.2fms/r)",
                         mapping, i, speed)

    def map_mapping(self, mapping, chunk_size=10000):
        """ Bulk load data to the appropriate tables. """
        ts = datetime.utcnow()
        conn = self.config.engine.connect()
        tx = conn.begin()
        properties = []
        entities = []
        try:
            for i, (s, p, o, t) in enumerate(self.records(mapping)):
                if p == TYPE_TYPE:
                    entities.append({
                        'subject': s,
                        'schema': o,
                        'source': self.generator.source,
                        'timestamp': ts
                    })
                    if len(entities) >= chunk_size:
                        self.config.entities.insert_bulk(conn, entities)
                        entities = []
                else:
                    properties.append({
                        'subject': s,
                        'predicate': p,
                        'object': o,
                        'type': t,
                        'source': self.generator.source,
                        'timestamp': ts
                    })
                    if len(properties) >= chunk_size:
                        self.config.properties.insert_bulk(conn, properties)
                        properties = []

                # flush transaction periodically.
                # not sure this is a good idea.
                if i > 0 and i % 1000000 == 0:
                    tx.commit()
                    tx = conn.begin()

            if len(properties):
                self.config.properties.insert_bulk(conn, properties)
            if len(entities):
                self.config.entities.insert_bulk(conn, entities)
            tx.commit()
        except:
            tx.rollback()
            raise

    def map(self):
        # TODO: delete all table contents.
        for mapping in self.generator.mappings:
            self.map_mapping(mapping)
