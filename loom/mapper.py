import time
import logging
from datetime import datetime

from loom.model import Binding, triplify, TYPE_TYPE
from loom.generator import Generator

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

    def map_mapping(self, conn, mapping):
        """ Bulk load data to the appropriate tables. """
        ts = datetime.utcnow()
        entities = self.config.entities.writer(conn)
        properties = self.config.properties.writer(conn)
        for i, (s, p, o, t) in enumerate(self.records(mapping)):
            if p == TYPE_TYPE:
                entities.write({
                    'subject': s,
                    'schema': o,
                    'source': self.generator.source,
                    'timestamp': ts
                })
            else:
                properties.write({
                    'subject': s,
                    'predicate': p,
                    'object': o,
                    'type': t,
                    'source': self.generator.source,
                    'timestamp': ts
                })
        properties.flush()
        entities.flush()

    def map(self):
        conn = self.config.engine.connect()
        tx = conn.begin()
        try:
            self.config.entities.delete_source(conn, self.generator.source)
            self.config.properties.delete_source(conn, self.generator.source)

            for mapping in self.generator.mappings:
                self.map_mapping(conn, mapping)
            tx.commit()

            self.config.entities.clean(conn)
            self.config.properties.clean(conn)
        except:
            tx.rollback()
            raise
