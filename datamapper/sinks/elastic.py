import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from datamapper.sinks.base import Sink
from datamapper.sinks.elastic_mapping import generate_mapping
from datamapper.util import ConfigException

log = logging.getLogger(__name__)

SOURCE_DOC_TYPE = 'source'


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

    def index_source(self, source):
        """ This is a work-around for the front-end to be able to easily get
        access to the sources: each source is indexed in a special document
        type. """
        self.client.index(index=self.index, doc_type=SOURCE_DOC_TYPE,
                          id=source.slug, body=source.to_dict())

    def make_doc_type(self, record):
        doc_type = self.config.get_alias(record.schema)
        if doc_type == SOURCE_DOC_TYPE:
            raise ConfigException("Invalid schema alias: %s" % doc_type)
        mapping = self.client.indices.get_mapping(index=self.index,
                                                  doc_type=doc_type)
        mapping = generate_mapping(mapping, self.index, doc_type, record,
                                   self.config.resolver)
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
                self.index_source(record.source)
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
