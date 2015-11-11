import logging
from datetime import datetime
from itertools import groupby

from sqlalchemy.sql.expression import select
from sqlalchemy.sql import bindparam
from jsonmapping import StatementsVisitor, TYPE_SCHEMA

log = logging.getLogger(__name__)


class EntityRight(object):
    """ An access control object for a given entity/statement. """

    def __init__(self, collections=None, sources=None, author=None):
        self.collections = set([c for c in collections if c is not None])
        self.sources = set([s for s in sources if s is not None])
        self.author = author

    def check(self, stmt):
        if self.collections is not None \
                and stmt.collection_id in self.collections:
            return True
        if self.sources is not None \
                and stmt.source_id in self.sources:
            return True
        if self.author is not None and stmt.author == self.author:
            return True
        return False


class EntityManager(object):
    """ Handle basic operations on entities. """

    def __init__(self, config):
        self.config = config
        self.visitors = {}

    def make_loader(self, right):
        """ This is used when loading an object. It will be called for the
        root entity and any nested entities, so it's a major performance
        bottleneck. """
        if not hasattr(self, '_pq'):
            table = self.config.properties.table
            q = select([table.c.predicate, table.c.object, table.c.type,
                        table.c.source_id, table.c.collection_id,
                        table.c.author])
            q = q.where(table.c.subject == bindparam('subject'))
            q = q.order_by(table.c.created_at.asc())
            self._pq = q.compile(self.config.engine)

        def _loader(subject):
            rp = self.config.engine.execute(self._pq, subject=subject)
            unique = set()
            for row in rp.fetchall():
                if right is not None and not right.check(row):
                    continue
                items = tuple(row.items())
                if items in unique:
                    continue
                unique.add(items)
                yield {
                    'predicate': row.predicate,
                    'object': row.object,
                    'type': row.type,
                    'source': row.source_id,
                    'collection': row.collection_id,
                    'author': row.author
                }
        return _loader

    def get_statements_visitor(self, schema_uri):
        """ This is a transformer object from ``jsonmapping`` that is used to
        transform entities from statement form to object form and vice versa.
        """
        if schema_uri not in self.visitors:
            _, schema = self.config.resolver.resolve(schema_uri)
            visitor = StatementsVisitor(schema, self.config.resolver,
                                        scope=self.config.base_uri)
            self.visitors[schema_uri] = visitor
        return self.visitors[schema_uri]

    def triplify(self, schema, data):
        """ Generate statements from a given data object. """
        visitor = self.get_statements_visitor(schema)
        return visitor.triplify(data)

    def _split_statements(self, statements, created_at, source_id=None,
                          collection_id=None, author=None):
        properties, types = [], []
        for (s, p, o, t) in statements:
            if p == TYPE_SCHEMA:
                types.append({
                    'subject': s,
                    'schema': o,
                    'source_id': source_id,
                    'collection_id': collection_id,
                    'author': author,
                    'created_at': created_at
                })
            else:
                properties.append({
                    'subject': s,
                    'predicate': p,
                    'object': o,
                    'type': t,
                    'source_id': source_id,
                    'collection_id': collection_id,
                    'author': author,
                    'created_at': created_at
                })
        return properties, types

    def _filter_properties(self, properties, right):
        loader = self.make_loader(right)
        for subject, props in groupby(properties, lambda p: p.get('subject')):
            current = {}
            for stmt in loader(subject):
                current[stmt['predicate']] = stmt['object']
            for prop in props:
                obj = current.get(prop['predicate'])
                if prop['predicate'] not in current or obj != prop['object']:
                    yield prop

    def _filter_types(self, types, right):
        for t in types:
            schema = self.get_schema(t['subject'], right=right)
            if schema is None:
                # TODO: potentially allow down-casting to more specific types.
                yield t

    def save(self, schema, data, source_id=None, collection_id=None,
             author=None, created_at=None, right=None):
        """ Save the given object to the database. """
        created_at = created_at or datetime.utcnow()
        statements = self.triplify(schema, data)
        properties, types = self._split_statements(statements, created_at,
                                                   source_id=source_id,
                                                   collection_id=collection_id,
                                                   author=author)
        properties = list(self._filter_properties(properties, right))
        types = list(self._filter_types(types, right))
        if len(types):
            self.config.types.insert_many(types)
        if len(properties):
            self.config.properties.insert_many(properties)
        if len(types):
            return types[0]['subject']
        return data.get('id')

    def get_schema(self, subject, right=None):
        """ For a given entity subject, return the appropriate schema. If this
        returns ``None``, the entity/subject does not exist. """
        table = self.config.types.table
        q = select([table.c.schema, table.c.collection_id, table.c.source_id,
                    table.c.author])
        q = q.where(table.c.subject == subject)
        q = q.order_by(table.c.created_at.desc())
        rp = self.config.engine.execute(q)
        for row in rp.fetchall():
            if right is not None and not right.check(row):
                continue
            return row.schema

    def get(self, subject, schema=None, depth=1, right=None):
        """ Get an object representation of an entity defined by the given
        ``schema`` and ``subject`` ID. """
        schema = schema or self.get_schema(subject, right=right)
        if schema is None:
            return
        visitor = self.get_statements_visitor(schema)
        loader = self.make_loader(right)
        return visitor.objectify(loader, subject, depth=depth)

    def subjects(self, schema=None, source_id=None, chunk=10000, right=None):
        """ Iterate over all entity IDs which match the current set of
        constraints (i.e. a specific schema or source dataset). """
        table = self.config.types.table
        q = select([table.c.subject, table.c.collection_id, table.c.source_id,
                    table.c.author])
        if schema is not None:
            q = q.where(table.c.schema == schema)
        if source_id is not None:
            q = q.where(table.c.source_id == source_id)
            log.info('Getting %s by source: %s', schema, source_id)
        else:
            log.info('Getting %s from all sources', schema)
        q = q.order_by(table.c.subject)
        rp = self.config.engine.execute(q)
        prev = None
        while True:
            rows = rp.fetchmany(chunk)
            if not len(rows):
                return
            for row in rows:
                if right is not None and not right.check(row):
                    continue
                if row.subject != prev:
                    yield row.subject
                    prev = row.subject
