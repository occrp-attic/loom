import logging
from time import time
# from datetime import datetime
from pprint import pprint  # noqa

from elasticsearch.helpers import bulk, scan

from loom.db import Source, session
from loom.analysis import extract_text, latinize
from loom.elastic import generate_mapping, BASE_SETTINGS

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
        body = {'mappings': mappings, 'settings': BASE_SETTINGS}
        client.indices.create(ignore=400, index=index, body=body)

    def convert_entity(self, subject, schema=None, depth=1):
        entity = self.config.entities.get(subject, schema=schema, depth=depth)
        # extend the object to index form
        entity['$text'] = extract_text(entity)
        entity['$latin'] = [latinize(t) for t in entity['$text']]
        entity['$suggest'] = entity.get('name')
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

    def is_schema_indexed(self, schema):
        if schema is None:
            return False
        # FIXME This is somewhat hacky, remove edge types:
        _, data = self.config.resolver.resolve(schema)
        if data.get('graph') == 'edge':
            return False
        if data.get('inline'):
            return False
        return True

    def clear(self, schema=None, source_id=None):
        filter_ = {'bool': {'must': []}}
        if schema is not None:
            filter_['bool']['must'].append({
                'term': {'$schema': schema}
            })
        if source_id is not None:
            filter_['bool']['must'].append({
                'term': {'$sources': source_id}
            })
        q = {'filtered': {'query': {'match_all': {}}, 'filter': filter_}}
        q = {'query': q, 'fields': []}

        log.info('Deleting existing entries matching index criteria')

        def gen_deletes():
            for res in scan(self.config.elastic_client, query=q,
                            index=self.config.elastic_index):
                yield {
                    '_op_type': 'delete',
                    '_index': self.config.elastic_index,
                    '_type': res.get('_type'),
                    '_id': res.get('_id')
                }

        bulk(self.config.elastic_client, gen_deletes(),
             stats_only=True, chunk_size=self.chunk,
             request_timeout=60.0)

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
        self.clear(schema=schema, source_id=source)
        schemas = self.config.schemas.values() if schema is None else [schema]
        for schema in schemas:
            if not self.is_schema_indexed(schema):
                continue
            bulk(client, self.generate_entities(schema, source),
                 stats_only=True, chunk_size=self.chunk,
                 request_timeout=60.0)
        client.indices.flush_synced()

    def index_one(self, subject, schema=None, depth=1):
        if schema is None:
            schema = self.config.entities.get_schema(subject)
        if not self.is_schema_indexed(schema):
            return
        entity = self.convert_entity(subject, schema=schema, depth=depth)
        self.config.elastic_client.index(index=entity.get('_index'),
                                         doc_type=entity.get('_type'),
                                         body=entity.get('_source'),
                                         id=entity.get('_id'))
        return entity
