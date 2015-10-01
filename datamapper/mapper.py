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

    def __init__(self, config, mapping, source):
        self.mapping = mapping
        self.config = config
        self.cache = []

        # TODO: make this more generic.
        self.source_pred = PRED['sources']
        self.source_obj = Literal(source)

        store = config.get('store')
        self.endpoint = store.get('data') or store.get('update')
        if self.endpoint is None:
            raise ConfigException("No store URL configured!")

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
        for t in self.add_sources(triples):
            self.cache.extend((t[0].n3(), t[1].n3(), t[2].n3(), '.'))

    @property
    def full(self):
        return len(self.cache) > 4 * 10000

    def flush(self):
        log.info("Uploading data to: %r.", self.endpoint)
        # headers = {'Content-Type': 'application/sparql-update'}
        headers = {'Content-Type': 'text/turtle'}
        data = ' '.join(self.cache).encode('utf-8')
        res = requests.post(self.endpoint, data=data, headers=headers)
        if res.status_code > 300:
            log.warning("Update error: %s", res.content)
        self.cache = []


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
                chunk.flush()

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
