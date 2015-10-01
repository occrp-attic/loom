import os
import time
import logging
import requests

from rdflib import Literal
from jsongraph.binding import Binding
from jsongraph.triplify import triplify
from jsongraph.vocab import PRED

from datamapper.generator import Generator
from datamapper.util import ConfigException

log = logging.getLogger(__name__)


class Chunk(object):
    """ A size-limited chunk of data, which is to be written to
    an output file. """

    def __init__(self, config, mapping, source, index=1):
        self.mapping = mapping
        self.source = source
        self.index = index
        self.config = config
        self.length = 0

        # TODO: make this more generic.
        self.source_pred = PRED['sources']
        self.source_obj = Literal(source)

        path = self.config.get('rdf_path')
        if path is None:
            raise ConfigException("No 'rdf_path' is configured.")
        file_name = '%s.%s.nt' % (mapping, index)
        self.path = os.path.join(path, source, file_name)
        try:
            os.makedirs(os.path.dirname(self.path))
        except:
            pass
        self.fh = open(self.path, 'w')

    def add_sources(self, triples):
        """ This is slightly hacky: I want all subjects to have provenance
        information attached, so this will just inspect the triples emitted
        by the generator and attach one sourcing predicate for each subject
        that has been seen. """
        subjects = set()
        for s, p, o in triples:
            subjects.add(s)
        for s in subjects:
            triples.append((s, self.source_pred, self.source_obj))
        return triples

    def write(self, triples):
        output = []
        for t in self.add_sources(triples):
            output.extend((t[0].n3(), t[1].n3(), t[2].n3(), '.\n'))
        text = ' '.join(output).encode('utf-8')
        self.length += len(text)
        self.fh.write(text)

    @property
    def full(self):
        return self.length > 30 * 1024

    def flush(self):
        self.fh.close()
        self.upload()

    def next(self):
        self.flush()
        return Chunk(self.config, self.mapping, self.source,
                     index=self.index + 1)

    def upload(self):
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


class Mapper(object):
    """ Map generated records to the data model. """

    def __init__(self, config, model):
        self.config = config
        endpoint = self.config.graph.store.update_endpoint
        self.config['rdf_endpoint'] = endpoint
        self.generator = Generator(config, model)

    def map_mapping(self, mapping):
        chunk = Chunk(self.config, mapping, self.generator.source)
        schema = None
        begin = time.time()
        for i, (schema_, data) in enumerate(self.generator.generate(mapping)):
            if schema is None:
                _, schema = self.config.resolver.resolve(schema_)

            if chunk.full:
                chunk = chunk.next()

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

    def map(self):
        for mapping in self.generator.mappings:
            self.map_mapping(mapping)
