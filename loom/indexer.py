import logging
from time import time
# from datetime import datetime
from pprint import pprint  # noqa

from elasticsearch.helpers import bulk

from loom.db import Source, session
from loom.analysis import extract_text, latinize
from loom.elastic import generate_mapping

log = logging.getLogger(__name__)


class Indexer(object):
    """ Index JSON/RDF to ElasticSearch. """

    def __init__(self, config):
        self.config = config
        self.chunk = int(config.get('chunk') or 1000)

    def configure(self):
        client = self.config.elastic_client
        index = self.config.elastic_index
        log.info("Ensuring search index and document mappings...")
        mappings = {}
        for schema in self.config.schemas.values():
            doc_type = self.config.get_alias(schema)
            mapping = generate_mapping(schema, self.config.resolver)
            mappings[doc_type] = mapping
        client.indices.create(ignore=400, index=index,
                              body={'mappings': mappings})

    def convert_entity(self, subject, schema=None):
        entity = self.config.entities.get(subject, schema=schema)
        # extend the object to index form
        entity['$text'] = extract_text(entity)
        entity['$latin'] = [latinize(t) for t in entity['$text']]
        # pprint(entity)
        return {
            '_id': entity.get('id'),
            '_type': self.config.get_alias(schema),
            '_index': self.config.elastic_index,
            '_source': entity
        }

    def generate_entities(self, schema, source_id):
        begin = time()
        subjects = self.config.entities.subjects(schema, source_id)
        for i, subject in enumerate(subjects):
            yield self.convert_entity(subject, schema=schema)
            if i > 0 and i % 1000 == 0:
                elapsed = time() - begin
                per_rec = (elapsed / float(i)) * 1000
                log.info("Indexing %r: %s records (%.2fms/r)",
                         schema, i, per_rec)

    def index(self, schema=None, source=None):
        if source is not None:
            q = session.query(Source.id).filter_by(slug=source)
            obj = q.first()
            if obj is None:
                raise ValueError("No such source: %s" % source)
            source = obj[0]
        client = self.config.elastic_client
        log.debug('Indexing to: %r (index: %r)', client,
                  self.config.elastic_index)
        schemas = self.config.schemas.values() if schema is None else [schema]
        for schema in schemas:
            bulk(client, self.generate_entities(schema, source),
                 stats_only=True, chunk_size=self.chunk,
                 request_timeout=60.0)
        client.indices.flush_synced()
