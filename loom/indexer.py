import logging
from time import time
# from datetime import datetime
from pprint import pprint  # noqa

from elasticsearch.helpers import bulk

from loom.analysis import extract_text, count_attrs, latinize
from loom.elastic import generate_mapping

log = logging.getLogger(__name__)


class Indexer(object):
    """ Index JSON/RDF to ElasticSearch. """

    def __init__(self, config):
        self.config = config
        self.chunk = int(config.get('chunk') or 1000)
        self.configure()

    def configure(self):
        client = self.config.elastic_client
        index = self.config.elastic_index
        log.info("Ensuring search index and document mappings...")
        client.indices.create(ignore=400, index=index)
        for schema in self.config.schemas.values():
            doc_type = self.config.get_alias(schema)
            mapping = generate_mapping(index, doc_type, schema,
                                       self.config.resolver)
            try:
                client.indices.put_mapping(index=index, doc_type=doc_type,
                                           body=mapping)
            except Exception as ex:
                log.warning("Cannot update data mapping: %s", ex)

    def convert_entity(self, subject, schema=None):
        entity = self.config.entities.get(subject, schema=schema)
        # extend the object to index form
        attr_count, link_count = count_attrs(entity)
        entity['$attrcount'] = attr_count
        entity['$linkcount'] = link_count
        entity['$text'] = extract_text(entity)
        entity['$latin'] = latinize(entity['$text'])
        # pprint(entity)
        return {
            '_id': entity.get('id'),
            '_type': self.config.get_alias(schema),
            '_index': self.config.elastic_index,
            '_source': entity
        }

    def generate_entities(self, schema, source):
        begin = time()
        subjects = self.config.entities.subjects(schema, source)
        for i, subject in enumerate(subjects):
            yield self.convert_entity(subject, schema=schema)
            if i > 0 and i % 1000 == 0:
                elapsed = time() - begin
                per_rec = (elapsed / float(i)) * 1000
                log.info("Indexing %r: %s records (%.2fms/r)",
                         schema, i, per_rec)

    def index(self, schema=None, source=None):
        client = self.config.elastic_client
        log.debug('Indexing to: %r (index: %r)', client,
                  self.config.elastic_index)
        schemas = self.config.schemas.values() if schema is None else [schema]
        for schema in schemas:
            bulk(client, self.generate_entities(schema, source),
                 stats_only=True, chunk_size=self.chunk,
                 request_timeout=60.0)
