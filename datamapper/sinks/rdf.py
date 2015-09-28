import os
import time
import logging
import requests

from jsongraph.binding import Binding
from jsongraph.triplify import triplify

from datamapper.sinks.base import Sink
from datamapper.util import ConfigException

log = logging.getLogger(__name__)


class Chunk(object):
    """ A size-limited chunk of data, which is to be written to
    an output file. """

    def __init__(self, config, mapping, record, index=1):
        self.mapping = mapping
        self.record = record
        self.index = index
        self.config = config
        self.length = 0

        path = self.config.get('rdf_path')
        if path is None:
            raise ConfigException("No 'rdf_path' is configured.")
        file_name = '%s.%s.nt' % (mapping, index)
        self.path = os.path.join(path, record.source.slug, file_name)
        try:
            os.makedirs(os.path.dirname(self.path))
        except:
            pass
        self.fh = open(self.path, 'w')

    def write(self, triples):
        output = []
        for t in triples:
            output.extend((t[0].n3(), t[1].n3(), t[2].n3(), '.\n'))
        text = ' '.join(output).encode('utf-8')
        self.length += len(text)
        self.fh.write(text)

    @property
    def full(self):
        return self.length > 30 * 1024 * 1024

    def flush(self):
        self.fh.close()
        self.sparql_submit()

    def next(self):
        self.flush()
        return Chunk(self.config, self.mapping, self.record,
                     index=self.index + 1)

    def sparql_submit(self):
        endpoint = self.config.get('rdf_endpoint')
        if endpoint is None:
            log.info("No 'rdf_endpoint', not uploading %r.", self.path)
            return

        log.info("Uploading data chunk %r to: %r.", self.path, endpoint)
        # headers = {'Content-Type': 'application/sparql-update'}
        headers = {'Content-Type': 'text/turtle'}
        with open(self.path, 'r') as fh:
            data = fh.read()
        res = requests.post(endpoint, data=data, headers=headers)
        if res.status_code > 300:
            log.warning("Update error: %s", res.content)


class RDFSink(Sink):
    """ Generate RDF files from the incoming records. """

    def load_mapping(self, mapping):
        chunk = None
        schema = None
        begin = time.time()
        for i, record in enumerate(self.generator.generate(mapping)):
            if chunk is None:
                chunk = Chunk(self.config, mapping, record)
                _, schema = self.config.resolver.resolve(record.schema)

            if chunk.full:
                chunk = chunk.next()

            data = record.entity
            binding = Binding(schema, self.config.resolver, data=data)
            _, triples = triplify(binding)
            chunk.write(triples)

            if i > 0 and i % 10000 == 0:
                elapsed = time.time() - begin
                per_record = float(elapsed) / float(i)
                speed = per_record * 1000
                log.info("Generating %r: %s records (%.2fms/r)",
                         mapping, i, speed)
        chunk.flush()

    def load(self):
        for mapping in self.generator.mappings:
            self.load_mapping(mapping)
