from datetime import datetime

from sqlalchemy import MetaData
from sqlalchemy.schema import Table, Column
from sqlalchemy.types import Unicode, BigInteger, DateTime


class StatementTable(object):
    """ The StatementTable manages writing and reading facts from the central
    fact table. """
    # TODO: in the future, we will probably also want a `links` and `types`
    # table.

    def __init__(self, engine):
        self.bind = engine
        self.table_name = '_statement'
        self.meta = MetaData()
        self.meta.bind = self.bind
        self._table = None

    @property
    def table(self):
        """ Generate an appropriate table representation to mirror the
        fields known for this table. """
        if self._table is None:
            self._table = Table(self.table_name, self.meta)
            col = Column('id', BigInteger, primary_key=True)
            self._table.append_column(col)
            col = Column('subject', Unicode(255))
            self._table.append_column(col)
            col = Column('predicate', Unicode(255))
            self._table.append_column(col)
            col = Column('object', Unicode())
            self._table.append_column(col)
            col = Column('source', Unicode(255))
            self._table.append_column(col)
            col = Column('timestamp', DateTime)
            self._table.append_column(col)
        return self._table

    @property
    def exists(self):
        return self.bind.has_table(self.table.name)

    def load_iter(self, iterable, chunk_size=10000):
        """ Bulk load data from an iterator to this table. """
        chunk = []
        ts = datetime.utcnow()
        conn = self.bind.connect()
        tx = conn.begin()
        try:
            for i, record in enumerate(iterable):
                record['timestamp'] = ts
                chunk.append(record)
                if len(chunk) >= chunk_size:
                    stmt = self.table.insert()
                    conn.execute(stmt, chunk)
                    chunk = []

            if len(chunk):
                stmt = self.table.insert()
                conn.execute(stmt, chunk)
            tx.commit()
        except:
            tx.rollback()
            raise

    def create(self):
        """ Create the table if it does not exist. """
        if not self.exists:
            self.table.create(self.bind)

    def drop(self):
        """ Drop the table if it does exist. """
        if self.exists:
            self.table.drop()
        self._table = None

    def __repr__(self):
        return "<StatementTable(%r)>" % (self.table_name)
