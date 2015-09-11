from sqlalchemy.schema import MetaData, Table
from datamapper.util import SpecException


class Transform(object):
    """ Apply a mapping specification to generate JSON schema data from a
    SQL database using field mappings. """

    def __init__(self, engine, spec):
        self.engine = engine
        self.metadata = MetaData()
        self.metadata.bind = self.engine
        self.spec = spec

    @property
    def tables(self):
        if not hasattr(self, '_tables'):
            self._tables = []
            for table_name in self.spec.get('tables', []):
                table = Table(table_name, self.metadata, autoload=True)
                self._tables.append(table)
        return self._tables

    def get_column(self, name):
        """ Get a column identified in the spec, e.g. as <table>.<column>. """
        table = None
        if '.' in name:
            table, name = name.split('.', 1)
        for t in self.tables:
            print t, t.name
            if t.name == table or table is None:
                if name not in t.columns:
                    raise SpecException("Invalid column: %s" % name)
                return t.columns[name]
        raise SpecException("Invalid table: %s" % table)
