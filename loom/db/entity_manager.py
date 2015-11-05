import logging

from sqlalchemy.sql.expression import select
from sqlalchemy.sql import bindparam
from jsonmapping import StatementsVisitor, TYPE_SCHEMA

log = logging.getLogger(__name__)


class EntityManager(object):
    """ Handle basic operations on entities. """

    def __init__(self, config):
        self.config = config
        self.visitors = {}

    def _load_properties(self, subject):
        """ This is used when loading an object. It will be called for the
        root entity and any nested entities, so it's a major performance
        bottleneck. """
        if not hasattr(self, '_pq'):
            table = self.config.properties.table
            q = select([table.c.predicate, table.c.object, table.c.type,
                        table.c.source_id, table.c.collection_id,
                        table.c.author])
            q = q.where(table.c.subject == bindparam('subject'))
            self._pq = q.compile(self.config.engine)

        rp = self.config.engine.execute(self._pq, subject=subject)
        unique = set()
        for row in rp.fetchall():
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

    def _split_statements(self, statements, source_id):
        properties, types = [], []
        for (s, p, o, t) in statements:
            if p == TYPE_SCHEMA:
                types.append({
                    'subject': s,
                    'schema': o,
                    'source_id': source_id
                })
            else:
                properties.append({
                    'subject': s,
                    'predicate': p,
                    'object': o,
                    'type': t,
                    'source_id': source_id
                })
        return properties, types

    def save(self, schema, data, source_id):
        """ Save the given object to the database. """
        statements = self.triplify(schema, data)
        properties, types = self._split_statements(statements, source_id)
        self.config.types.insert_many(types)
        self.config.properties.insert_many(properties)
        if len(types):
            return types[0]

    def get_schema(self, subject):
        """ For a given entity subject, return the appropriate schema. If this
        returns ``None``, the entity/subject does not exist. """
        table = self.config.types.table
        q = select([table.c.schema])
        q = q.where(table.c.subject == subject)
        # TODO: sorting
        rp = self.config.engine.execute(q)
        row = rp.fetchone()
        if row is None:
            return None
        return row.schema

    def get(self, subject, schema=None, depth=1):
        """ Get an object representation of an entity defined by the given
        ``schema`` and ``subject`` ID. """
        schema = schema or self.get_schema(subject)
        if schema is None:
            return
        visitor = self.get_statements_visitor(schema)
        return visitor.objectify(self._load_properties, subject, depth=depth)

    def subjects(self, schema=None, source_id=None, chunk=10000):
        """ Iterate over all entity IDs which match the current set of
        constraints (i.e. a specific schema or source dataset). """
        table = self.config.types.table
        q = select([table.c.subject])
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
                if row.subject != prev:
                    yield row.subject
                    prev = row.subject
