import logging
from datetime import datetime

from normality import slugify
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from datamapper.sinks.base import Sink
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
        doc_type = '%s-%s' % (record.source.slug, record.mapping)
        doc_type = slugify(doc_type)

        return doc_type

    def actions(self):
        indexed_at = datetime.utcnow()
        doc_type = None
        for record in self.records():
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

    def load(self):
        log.info('Indexing to: %r (index: %r)',
                 self.config.get('elastic_host'), self.index)
        self.client.indices.create(index=self.index, ignore=400)
        # TODO generate a mapping.
        bulk(self.client, self.actions(), stats_only=True,
             chunk_size=1000)

    def clear(self):
        q = {}
        self.client.delete_by_query(index=self.index, doc_type=self.doc_type,
                                    body=q)
