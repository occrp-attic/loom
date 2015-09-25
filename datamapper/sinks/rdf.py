import os
import time
import logging

from jsongraph.binding import Binding
from jsongraph.triplify import triplify

from datamapper.sinks.base import Sink
from datamapper.util import ConfigException

log = logging.getLogger(__name__)


class Writer(object):

    def __init__(self, config, mapping, record):
        self.config = config
        self.mapping = mapping
        self.record = record


class TurtleWriter(Writer):

    def __init__(self, config, mapping, record):
        path = config.get('rdf_path')
        if path is None:
            raise ConfigException("No 'rdf_path' is configured.")
        file_name = '%s.nt' % mapping
        path = os.path.join(path, record.source.slug, file_name)
        try:
            os.makedirs(os.path.dirname(path))
        except:
            pass
        self.fh = open(path, 'w')

    def write(self, triples):
        output = []
        for t in triples:
            output.extend((t[0].n3(), t[1].n3(), t[2].n3(), '.\n'))
        self.fh.write(' '.join(output).encode('utf-8'))

    def flush(self):
        self.fh.close()


class RDFSink(Sink):
    """ Generate RDF files from the incoming records. """

    def load_mapping(self, mapping):
        writer = None
        schema = None
        begin = time.time()
        for i, record in enumerate(self.generator.generate(mapping)):
            if writer is None:
                writer = TurtleWriter(self.config, mapping, record)
                _, schema = self.config.resolver.resolve(record.schema)

            data = record.entity
            binding = Binding(schema, self.config.resolver, data=data)
            _, triples = triplify(binding)
            writer.write(triples)

            if i > 0 and i % 10000 == 0:
                elapsed = time.time() - begin
                per_record = float(elapsed) / float(i)
                speed = per_record * 1000
                log.info("Generating %r: %s records (%.2fms/r)",
                         mapping, i, speed)
        writer.flush()

    def load(self):
        for mapping in self.generator.mappings:
            self.load_mapping(mapping)
