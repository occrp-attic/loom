import logging
from datetime import datetime

from normality import slugify
from jsonmapping import SchemaVisitor
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from datamapper.sinks.base import Sink
from datamapper.sinks.elastic_mapping import BASE_MAPPING
from datamapper.sinks.elastic_mapping import generate_schema_mapping
from datamapper.util import ConfigException

log = logging.getLogger(__name__)


class ElasticSink(Sink):
    """ Index the emitted records in ElasticSearch. """

    @property
    def client(self):
        if not hasattr(self, '_client'):
            host = self.config.get('elastic_host')
            if host is None:
                raise ConfigException("No 'elastic_host' is configured.")
            self._client = Elasticsearch([host])
        return self._client

    @property
    def index(self):
        if not hasattr(self, '_index'):
            self._index = self.config.get('elastic_index')
            if self._index is None:
                raise ConfigException("No 'elastic_index' is configured.")
        return self._index

    def make_doc_type(self, record):
        doc_type = self.config.get_alias(record.schema)
        mapping = self.client.indices.get_mapping(index=self.index,
                                                  doc_type=doc_type)
        mapping = mapping.get(self.index, {}).get('mappings', {})
        mapping = mapping.get(doc_type, BASE_MAPPING)

        visitor = SchemaVisitor({'$ref': record.schema}, self.config.resolver)
        entity = generate_schema_mapping(visitor, set())
        mapping['properties']['entity'] = entity
        try:
            self.client.indices.put_mapping(index=self.index,
                                            doc_type=doc_type,
                                            body={doc_type: mapping})
        except Exception as ex:
            log.warning("Cannot update data mapping: %s", ex)
        return doc_type

    def actions(self):
        indexed_at = datetime.utcnow()
        doc_type = None
        for i, record in enumerate(self.records()):
            if doc_type is None:
                doc_type = self.make_doc_type(record)
            data = record.to_dict()
            data['indexed_at'] = indexed_at
            yield {
                '_id': record.id,
                '_type': doc_type,
                '_index': self.index,
                '_source': data
            }

            if i > 0 and i % 10000 == 0:
                log.info("Indexing to %s: %s records", doc_type, i)

    def load(self):
        log.info('Indexing to: %r (index: %r)',
                 self.config.get('elastic_host'), self.index)
        self.client.indices.create(index=self.index, ignore=400)
        bulk(self.client, self.actions(), stats_only=True,
             chunk_size=1000)

    def clear(self):
        pass
        # TODO: how to do this properly.
        # q = {}
        # self.client.delete_by_query(index=self.index, doc_type=self.doc_type,
        #                             body=q)
