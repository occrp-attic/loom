import logging
from time import time
# from datetime import datetime
from pprint import pprint  # noqa

from sqlalchemy.sql.expression import select
from sqlalchemy.sql import bindparam
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from loom.util import ConfigException
from loom.elastic import generate_mapping
from loom.model import Binding, objectify

log = logging.getLogger(__name__)


class Indexer(object):
    """ Index JSON/RDF to ElasticSearch. """

    def __init__(self, config):
        self.config = config
        self.chunk = int(config.get('chunk') or 1000)

    @property
    def client(self):
        if not hasattr(self, '_client'):
            host = self.config.get('elastic_host')
            if host is None:
                raise ConfigException("No 'elastic_host' is configured.")
            self._client = Elasticsearch([host])
        return self._client

    @property
    def index_name(self):
        if not hasattr(self, '_index_name'):
            self._index_name = self.config.get('elastic_index')
            if self._index_name is None:
                raise ConfigException("No 'elastic_index' is configured.")
        return self._index_name

    def generate_subjects(self, schema):
        """ Iterate over all entity IDs which match the current set of
        constraints (i.e. a specific schema or source dataset). """
        table = self.config.entities.table
        q = select([table.c.subject])
        q = q.where(table.c.schema == schema)
        q = q.where(table.c.source == self.config.source)
        log.info('Getting %s by source: %s', schema, self.config.source)
        rp = self.config.engine.execute(q)
        while True:
            rows = rp.fetchmany(self.chunk)
            if not len(rows):
                return
            for row in rows:
                yield row.subject

    def properties_of(self, subject):
        if not hasattr(self, '_pq'):
            table = self.config.properties.table
            q = select([table.c.predicate, table.c.object, table.c.source])
            q = q.where(table.c.subject == bindparam('subject'))
            self._pq = q.compile(self.config.engine)

        rp = self.config.engine.execute(self._pq, subject=subject)
        for row in rp.fetchall():
            # TODO: do we need type casting here?
            yield row.predicate, row.object, row.source

    def generate_entities(self, schema_uri):
        begin = time()
        _, schema = self.config.resolver.resolve(schema_uri)
        binding = Binding(schema, self.config.resolver)
        doc_type = self.config.get_alias(schema_uri)
        for i, subject in enumerate(self.generate_subjects(schema=schema_uri)):
            entity = objectify(self.properties_of, subject, binding, 4, set())
            yield {
                '_id': entity.get('id'),
                '_type': doc_type,
                '_index': self.index_name,
                '_source': entity
            }
            if i > 0 and i % 1000 == 0:
                elapsed = time() - begin
                per_rec = (elapsed / float(i)) * 1000
                log.info("Indexing %r: %s records (%.2fms/r)",
                         schema_uri, i, per_rec)

    def make_doc_type(self, schema):
        doc_type = self.config.get_alias(schema)
        mapping = self.client.indices.get_mapping(index=self.index_name,
                                                  doc_type=doc_type)
        mapping = generate_mapping(mapping, self.index_name, doc_type, schema,
                                   self.config.resolver)
        try:
            self.client.indices.put_mapping(index=self.index_name,
                                            doc_type=doc_type,
                                            body={doc_type: mapping})
        except Exception as ex:
            log.warning("Cannot update data mapping: %s", ex)
        return doc_type

    def index(self):
        self.client.indices.create(index=self.index_name, ignore=400)
        log.info('Indexing to: %r (index: %r)',
                 self.config.get('elastic_host'), self.index_name)
        for alias, schema in self.config.schemas.items():
            self.make_doc_type(schema)
            bulk(self.client, self.generate_entities(schema),
                 stats_only=True, chunk_size=self.chunk)
