import os
import logging

from jsongraph.binding import Binding
from jsongraph.triplify import triplify

from datamapper.sinks.base import Sink
from datamapper.util import ConfigException

log = logging.getLogger(__name__)


class RDFSink(Sink):
    """ Generate RDF files from the incoming records. """

    def get_path(self, mapping, record):
        path = self.config.get('rdf_path')
        if path is None:
            raise ConfigException("No 'rdf_path' is configured.")
        file_name = '%s.ttl' % mapping
        return os.path.join(path, record.source.slug, file_name)

    def emit_to_file(self, fh, triples):
        output = []
        for t in triples:
            output.extend((t[0].n3(), t[1].n3(), t[2].n3(), '.\n'))
        fh.write(' '.join(output).encode('utf-8'))

    def load_mapping(self, mapping):
        outfh = None
        path = None
        schema = None
        import time
        begin = time.time()
        for i, record in enumerate(self.generator.generate(mapping)):
            if path is None:
                path = self.get_path(mapping, record)
                try:
                    os.makedirs(os.path.dirname(path))
                except:
                    pass
                outfh = open(path, 'w')
                _, schema = self.config.resolver.resolve(record.schema)

            data = record.entity
            binding = Binding(schema, self.config.resolver, data=data)
            _, triples = triplify(binding)

            if i > 0 and i % 10000 == 0:
                elapsed = time.time() - begin
                per_record = float(elapsed) / float(i)
                speed = per_record * 1000
                log.info("Generaring %r RDF: %s records (speed: %s)", mapping, i, speed)

        outfh.close()

    def load(self):
        for mapping in self.generator.mappings:
            self.load_mapping(mapping)
