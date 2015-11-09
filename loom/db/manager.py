import logging

from sqlalchemy import func
from sqlalchemy.sql.expression import select

from loom.db.writer import Writer

log = logging.getLogger(__name__)


class TableManager(object):
    """ The table manager manages writing and reading data from SQL tables. """

    def __init__(self, config, clazz):
        self.config = config
        self.cls = clazz
        self.table = clazz.__table__
        self.bind = config.engine
        self.name = clazz.__tablename__

    def writer(self):
        return Writer(self)

    def insert_many(self, rows, bind=None):
        """ Insert a bunch of rows into the table. """
        conn = bind or self.bind.connect()
        stmt = self.table.insert()
        conn.execute(stmt, rows)

    def delete(self, **kwargs):
        q = self.table.delete()
        for column, value in kwargs.items():
            if value is not None:
                q = q.where(self.table.c[column] == value)
        conn = self.bind.connect()
        tx = conn.begin()
        conn.execute(q)
        tx.commit()

    def __len__(self):
        q = select(columns=[func.count(True)], from_obj=self.table)
        rp = self.bind.connect().execute(q)
        return rp.scalar()

    def __repr__(self):
        return "<TableManager(%r)>" % (self.name)
