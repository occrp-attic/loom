import logging

from sqlalchemy.sql.expression import select
from sqlalchemy.sql import bindparam
from jsonmapping import StatementsVisitor

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
            q = select([table.c.predicate, table.c.object, table.c.source])
            q = q.where(table.c.subject == bindparam('subject'))
            self._pq = q.compile(self.config.engine)

        rp = self.config.engine.execute(self._pq, subject=subject)
        rows = rp.fetchall()
        rows = [(row.predicate, row.object, row.source) for row in rows]
        return set(rows)

    def get_statements_visitor(self, schema_uri):
        """ This is a transformer object from ``jsonmapping`` that is used to
        transform entities from statement form to object form and vice versa.
        """
        if schema_uri not in self.visitors:
            _, schema = self.config.resolver.resolve(schema_uri)
            visitor = StatementsVisitor(schema, self.config.resolver)
            self.visitors[schema_uri] = visitor
        return self.visitors[schema_uri]

    def get(self, subject, schema=None):
        """ Get an object representation of an entity defined by the given
        ``schema`` and ``subject`` ID. """
        if schema is None:
            table = self.config.types.table
            q = select([table.c.schema])
            q = q.where(table.c.subject == subject)
            # TODO: sorting
            rp = self.config.engine.execute(q)
            schema = rp.fetchone().schema
        visitor = self.get_statements_visitor(schema)
        return visitor.objectify(self._load_properties, subject)

    def subjects(self, schema=None, source=None, chunk=10000):
        """ Iterate over all entity IDs which match the current set of
        constraints (i.e. a specific schema or source dataset). """
        table = self.config.types.table
        q = select([table.c.subject])
        if schema is not None:
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
            rows = rp.fetchmany(chunk)
            if not len(rows):
                return
            for row in rows:
                if row.subject != prev:
                    yield row.subject
                    prev = row.subject
