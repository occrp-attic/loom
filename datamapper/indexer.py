import logging
from time import time
from itertools import count
from pprint import pprint  # noqa

from jsongraph import Graph
from sparqlquery import Select, v, asc
from rdflib import RDF, URIRef

log = logging.getLogger(__name__)


class Indexer(object):
    """ Index JSON/RDF to ElasticSearch. """

    def __init__(self, config, model):
        self.chunk = int(config.get('chunk') or 1000)
        self.config = config
        self.graph = Graph(config=config, resolver=config.resolver)

    def generate_subjects(self, schema=None):
        """ Iterate over all entity IDs which match the current set of
        constraints (i.e. a specific schema or source dataset). """
        subj = v.subject
        q = Select([subj])
        if schema is not None:
            q = q.where((subj, RDF.type, URIRef(schema)))
        q = q.order_by(asc(subj))
        limit = self.chunk * 10
        q = q.limit(limit)
        for i in count(0):
            lq = q.offset(i * limit)
            log.info('Getting entity IDs: %s', lq.compile().replace('\n', ' '))
            for subject, in lq.execute(self.config.graph.graph):
                yield subject
            else:
                return

    def index(self):
        for alias, schema in self.config.schemas.items():
            begin = time()
            for i, subject in enumerate(self.generate_subjects(schema=schema)):
                entity = self.config.graph.get(subject, schema=schema, depth=3)
                if i > 0 and i % 100 == 0:
                    elapsed = time() - begin
                    per_rec = (elapsed / float(i)) * 1000
                    print i, per_rec
