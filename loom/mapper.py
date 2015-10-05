import time
import logging
from datetime import datetime

from loom.model import Extractor, TYPE_TYPE
from loom.db.generator import Generator

log = logging.getLogger(__name__)


class Mapper(object):
    """ Map generated records to the data model. """

    def __init__(self, config):
        self.config = config
        self.generator = Generator(config)

    def records(self, mapping_name):
        extractor = Extractor(self.config.get_mapping(mapping_name),
                              self.config.resolver, self.config.base_uri)
        begin = time.time()
        for i, row in enumerate(self.generator.generate(mapping_name)):
            for stmt in extractor.extract(row):
                yield stmt

            if i > 0 and i % 10000 == 0:
                elapsed = time.time() - begin
                per_record = float(elapsed) / float(i)
                speed = per_record * 1000
                log.info("Generating %r: %s records (%.2fms/r)",
                         mapping_name, i, speed)

    def map_mapping(self, conn, mapping):
        """ Bulk load data to the appropriate tables. """
        ts = datetime.utcnow()
        entities = self.config.entities.writer(conn)
        properties = self.config.properties.writer(conn)
        for i, (s, p, o, t) in enumerate(self.records(mapping)):
            continue
            if p == TYPE_TYPE:
                entities.write({
                    'subject': s,
                    'schema': o,
                    'source': self.config.source,
                    'timestamp': ts
                })
            else:
                properties.write({
                    'subject': s,
                    'predicate': p,
                    'object': o,
                    'type': t,
                    'source': self.config.source,
                    'timestamp': ts
                })
        properties.flush()
        entities.flush()

    def map(self):
        conn = self.config.engine.connect()
        tx = conn.begin()
        try:
            self.config.entities.delete_source(conn, self.config.source)
            self.config.properties.delete_source(conn, self.config.source)

            for mapping in self.config.mappings:
                self.map_mapping(conn, mapping)
            tx.commit()

            self.config.entities.clean(conn)
            self.config.properties.clean(conn)
        except:
            tx.rollback()
            raise
