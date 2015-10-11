import time
import logging
from datetime import datetime

from jsonmapping import Mapper as SchemaMapper

from loom.model import triplify, Binding, TYPE_TYPE
from loom.db.generator import Generator

log = logging.getLogger(__name__)


class Mapper(object):
    """ Map generated records to the data model. """

    def __init__(self, config):
        self.config = config
        self.generator = Generator(config)

    def records(self, mapping_name):
        mapper = SchemaMapper(self.config.get_mapping(mapping_name),
                              self.config.resolver, scope=self.config.base_uri)
        binding = Binding(mapper.visitor.schema, self.config.resolver,
                          scope=self.config.base_uri)
        begin = time.time()
        stmts = 0
        for i, row in enumerate(self.generator.generate(mapping_name)):
            _, data = mapper.apply(row)
            for stmt in triplify(binding, data, None):
                stmts += 1
                yield stmt

            if i > 0 and i % 10000 == 0:
                elapsed = time.time() - begin
                per_record = float(elapsed) / float(i)
                speed = per_record * 1000
                log.info("Generating %r: %s records (%s, %.2fms/r)",
                         mapping_name, i, stmts, speed)

    def map_mapping(self, mapping):
        """ Bulk load data to the appropriate tables. """
        entities = self.config.entities.writer()
        properties = self.config.properties.writer()
        for i, (s, p, o, t) in enumerate(self.records(mapping)):
            if p == TYPE_TYPE:
                entities.write({
                    'subject': s,
                    'schema': o,
                    'source': self.config.source
                })
            else:
                properties.write({
                    'subject': s,
                    'predicate': p,
                    'object': o,
                    'type': t,
                    'source': self.config.source
                })

        properties.flush()
        entities.flush()

    def map(self):
        for mapping in self.config.mappings:
            self.map_mapping(mapping)
