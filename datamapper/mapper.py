import time
import logging

from datamapper.model import Binding, triplify
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
            _, triples = triplify(binding)
            for triple in triples:
                yield {
                    'subject': triple[0].n3(),
                    'predicate': triple[1],
                    'object': triple[2].n3(),
                    'source': self.generator.source,
                }

            if i > 0 and i % 10000 == 0:
                elapsed = time.time() - begin
                per_record = float(elapsed) / float(i)
                speed = per_record * 1000
                log.info("Generating %r: %s records (%.2fms/r)",
                         mapping, i, speed)

    def map_mapping(self, mapping):
        self.config.statement.load_iter(self.records(mapping))

    def map(self):
        # TODO: delete all table contents.
        for mapping in self.generator.mappings:
            self.map_mapping(mapping)
