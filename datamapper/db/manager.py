import logging

from sqlalchemy.schema import Table

log = logging.getLogger(__name__)


class TableManager(object):
    """ The table manager manages writing and reading data from SQL tables. """

    def __init__(self, meta, name, columns, indexes):
        self.bind = meta.bind
        self.meta = meta
        self.name = name
        self.columns = columns
        self.indexes = columns

    @property
    def table(self):
        """ Generate an appropriate table representation to mirror the
        fields known for this table. """
        if not hasattr(self, '_table'):
            self._table = Table(self.name, self.meta)
            for col in self.columns:
                self._table.append_column(col)
        return self._table

    @property
    def exists(self):
        return self.bind.has_table(self.table.name)

    def ensure(self):
        self.create()
        return self

    def insert_bulk(self, conn, rows):
        """ Bulk load data from an iterator to this table. """
        stmt = self.table.insert()
        conn.execute(stmt, rows)

    def create(self):
        """ Create the table if it does not exist. """
        if not self.exists:
            log.info("Creating table: %r in %r", self.name, self.bind)
            self.table.create(self.bind)

    def drop(self):
        """ Drop the table if it does exist. """
        if self.exists:
            self.table.drop()
        self._table = None

    def __repr__(self):
        return "<TableManager(%r)>" % (self.name)
