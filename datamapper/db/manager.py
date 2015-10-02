import logging

from sqlalchemy.schema import Table, Index

log = logging.getLogger(__name__)


class TableManager(object):
    """ The table manager manages writing and reading data from SQL tables. """

    def __init__(self, meta, name, columns, indexes):
        self.bind = meta.bind
        self.meta = meta
        self.name = name
        self.columns = columns
        self.indexes = indexes

    @property
    def table(self):
        """ Generate an appropriate table representation to mirror the
        fields known for this table. """
        if not hasattr(self, '_table'):
            if self.bind.has_table(self.name):
                self._table = Table(self.name, self.meta, autoload=True)
            else:
                self._table = Table(self.name, self.meta)
                for col in self.columns:
                    self._table.append_column(col)
                log.info("Creating table: %r in %r", self.name, self.bind)
                self._table.create(self.bind)
            self._create_indexes(self._table)
        return self._table

    def _create_indexes(self, table):
        existing = [i.name for i in table.indexes]
        for columns in self.indexes:
            index = '_'.join([self.name] + list(columns) + ['idx'])
            if index in existing:
                continue
            log.info("Adding DB index %r: %r", self.name, columns)
            columns = [table.c[c] for c in columns]
            index = Index(index, *columns)
            index.create(bind=self.bind)

    @property
    def exists(self):
        return self.bind.has_table(self.table.name)

    def insert_bulk(self, conn, rows):
        """ Bulk load data from an iterator to this table. """
        stmt = self.table.insert()
        conn.execute(stmt, rows)

    def drop(self):
        """ Drop the table if it does exist. """
        if self.exists:
            self.table.drop()
        self._table = None

    def __repr__(self):
        return "<TableManager(%r)>" % (self.name)
