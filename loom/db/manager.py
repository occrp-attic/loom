import logging

from sqlalchemy import and_, select, func
from sqlalchemy.schema import Table, Index
from sqlalchemy.schema import MetaData

from loom.db.writer import Writer

log = logging.getLogger(__name__)


class TableManager(object):
    """ The table manager manages writing and reading data from SQL tables. """

    def __init__(self, config, name, columns, indexes, unique):
        self.config = config
        self.bind = config.engine
        self.meta = MetaData()
        self.meta.bind = config.engine
        self.name = name
        self.columns = columns
        self.indexes = indexes
        self.unique = unique

        # access to create
        bind = self.bind.connect()
        if self.is_postgresql:
            bind.connection.connection.set_isolation_level(0)

        self.table = Table(self.name, self.meta)
        for col in self.columns:
            self.table.append_column(col)
        if not self.bind.has_table(self.name):
            log.info("Creating table: %r in %r", self.name, bind)
            self.table.create(bind)

        self._create_indexes(bind)

    @property
    def is_postgresql(self):
        return 'postgres' in self.bind.dialect.name

    def _create_indexes(self, bind):
        existing = [i.name for i in self.table.indexes]
        for columns in self.indexes:
            index = '_'.join([self.name] + list(columns) + ['idx'])
            if index in existing:
                continue
            log.debug("Adding index %r: %r", self.name, columns)
            columns = [self.table.c[c] for c in columns]
            index = Index(index, *columns, postgresql_concurrently=True)
            index.create(bind=bind)

    def writer(self):
        return Writer(self)

    def insert_many(self, rows, bind=None):
        """ Insert a bunch of rows into the table. """
        conn = bind or self.bind.connect()
        stmt = self.table.insert()
        conn.execute(stmt, rows)

    def upsert(self, record):
        """ Check if a given record exists, otherwise insert it. """
        wc = [self.table.c[u] == record.get(u) for u in self.unique]
        wc = and_(*wc)
        q = select([self.table.columns.id]).where(wc).count()
        if self.bind.execute(q).scalar() > 0:
            q = self.table.update().where(wc).values(record)
        else:
            q = self.table.insert(record)
        self.bind.execute(q)

    def delete(self, **kwargs):
        q = self.table.delete()
        for column, value in kwargs.items():
            q = q.where(self.table.c[column] == value)
        conn = self.bind.connect()
        tx = conn.begin()
        conn.execute(q)
        tx.commit()

    def dedupe(self):
        # Requires window functions.
        if not self.is_postgresql:
            return
        log.info("De-duplicating table: %r", self.name)
        args = {
            'name': self.name,
            'unique': ', '.join(self.unique)
        }
        dedupe_q = """
            DELETE FROM %(name)s WHERE id IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (partition BY %(unique)s
                    ORDER BY id) AS rnum
                FROM %(name)s) t
                WHERE t.rnum > 1);
        """ % args
        conn = self.bind.connect()
        tx = conn.begin()
        conn.execute(dedupe_q)
        tx.commit()

    def __len__(self):
        rp = self.bind.connect().execute(self.table.select().count())
        return rp.scalar()

    def __repr__(self):
        return "<TableManager(%r)>" % (self.name)
