import time
import logging

from jsonmapping import Mapper as SchemaMapper
from jsonmapping import StatementsVisitor, TYPE_SCHEMA
from loom.db.generator import Generator

log = logging.getLogger(__name__)


class Mapper(object):
    """ Map generated records to the data model. """

    def __init__(self, config, spec):
        self.config = config
        self.spec = spec
        self.generator = Generator(spec)

    def records(self, mapping_name):
        mapper = SchemaMapper(self.spec.get_mapping(mapping_name),
                              self.config.resolver, scope=self.config.base_uri)
        schema = mapper.visitor.schema.get('id')
        begin = time.time()
        stmts = 0
        for i, row in enumerate(self.generator.generate(mapping_name)):
            _, data = mapper.apply(row)
            for stmt in self.config.entities.triplify(schema, data):
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
        types = self.config.types.writer()
        properties = self.config.properties.writer()
        for (s, p, o, t) in self.records(mapping):
            if p == TYPE_SCHEMA:
                types.write({
                    'subject': s,
                    'schema': o,
                    'source': self.spec.source
                })
            else:
                properties.write({
                    'subject': s,
                    'predicate': p,
                    'object': o,
                    'type': t,
                    'source': self.spec.source
                })
        properties.flush()
        types.flush()

    def map(self):
        for mapping in self.spec.mappings:
            self.map_mapping(mapping)
