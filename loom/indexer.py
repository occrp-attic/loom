import logging
from time import time
# from datetime import datetime
from pprint import pprint  # noqa

from sqlalchemy.sql.expression import select
from sqlalchemy.sql import bindparam
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from jsonmapping import StatementsVisitor

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
        self.config.elastic_client.indices.create(ignore=400,
            index=self.config.elastic_index)  # noqa

    def generate_subjects(self, schema, source=None):
        """ Iterate over all entity IDs which match the current set of
        constraints (i.e. a specific schema or source dataset). """
        table = self.config.entities.table
        q = select([table.c.subject])
        q = q.where(table.c.schema == schema)
        if source is not None:
            q = q.where(table.c.source == source)
            log.info('Getting %s by source: %s', schema, source)
        else:
            log.info('Getting %s from all sources', schema)
        q = q.order_by(table.c.subject)
        rp = self.config.engine.execute(q)
        prev = None
        while True:
            rows = rp.fetchmany(self.chunk)
            if not len(rows):
                return
            for row in rows:
                if row.subject != prev:
                    yield row.subject
                    prev = row.subject

    def properties_of(self, subject):
        if not hasattr(self, '_pq'):
            table = self.config.properties.table
            q = select([table.c.predicate, table.c.object, table.c.source])
            q = q.where(table.c.subject == bindparam('subject'))
            self._pq = q.compile(self.config.engine)

        rp = self.config.engine.execute(self._pq, subject=subject)
        rows = rp.fetchall()
        rows = [(row.predicate, row.object, row.source) for row in rows]
        return set(rows)

    def generate_entities(self, schema, source):
        begin = time()
        _, schema_data = self.config.resolver.resolve(schema)
        statements = StatementsVisitor(schema_data, self.config.resolver)
        doc_type = self.config.get_alias(schema)
        for i, subject in enumerate(self.generate_subjects(schema, source)):
            entity = statements.objectify(self.properties_of, subject)
            # pprint(entity)

            # extend the object to index form
            attr_count, link_count = count_attrs(entity)
            entity['$attrcount'] = attr_count
            entity['$linkcount'] = link_count
            entity['$text'] = extract_text(entity)
            entity['$latin'] = latinize(entity['$text'])
            # pprint(entity)

            yield {
                '_id': entity.get('id'),
                '_type': doc_type,
                '_index': self.config.elastic_index,
                '_source': entity
            }
            if i > 0 and i % 1000 == 0:
                elapsed = time() - begin
                per_rec = (elapsed / float(i)) * 1000
                log.info("Indexing %r: %s records (%.2fms/r)",
                         doc_type, i, per_rec)

    def make_doc_type(self, schema):
        client = self.config.elastic_client
        index = self.config.elastic_index
        doc_type = self.config.get_alias(schema)
        mapping = client.indices.get_mapping(index=index, doc_type=doc_type)
        mapping = generate_mapping(mapping, index, doc_type, schema,
                                   self.config.resolver)
        try:
            client.indices.put_mapping(index=index, doc_type=doc_type,
                                       body={doc_type: mapping})
        except Exception as ex:
            log.warning("Cannot update data mapping: %s", ex)
        return doc_type

    def index(self, schema, source):
        client = self.config.elastic_client
        log.debug('Indexing to: %r (index: %r)', client,
                  self.config.elastic_index)
        schemas = self.config.schemas.values() if schema is None else [schema]
        for schema in schemas:
            self.make_doc_type(schema)
            bulk(client, self.generate_entities(schema, source),
                 stats_only=True, chunk_size=self.chunk,
                 request_timeout=60.0)
