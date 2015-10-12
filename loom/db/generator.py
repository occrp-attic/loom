import logging
from collections import Mapping

from sqlalchemy.schema import Table, MetaData
from sqlalchemy.sql.expression import select

from loom.util import SpecException

log = logging.getLogger(__name__)


class Generator(object):
    """ Apply a mapping specification to generate JSON schema data from a
    SQL database using field mappings. """

    def __init__(self, config):
        self.config = config
        self.metadata = MetaData()
        self.metadata.bind = self.config.ods

    @property
    def tables(self):
        if not hasattr(self, '_tables'):
            self._tables = []
            for table_obj in self.config.get('tables', []):
                table_name, table_alias = table_obj, None
                if isinstance(table_obj, Mapping):
                    table_name = table_obj.get('table')
                    table_alias = table_obj.get('alias')

                table = Table(table_name, self.metadata, autoload=True)
                if table_alias is not None:
                    table = table.alias(table_alias)

                self._tables.append(table)
        return self._tables

    @property
    def joins(self):
        if not hasattr(self, '_joins'):
            self._joins = []
            for join in self.config.get('joins', {}):
                for left, right in join.items():
                    self._joins.append((self.get_column(left),
                                        self.get_column(right)))
        return self._joins

    def _get_table(self, name):
        table = None
        if '.' in name:
            table, name = name.split('.', 1)
        for t in self.tables:
            if t.name == table or table is None:
                return t
        raise SpecException("Invalid table: %s" % table)

    def get_column(self, name):
        """ Get a column identified in the spec, e.g. as <table>.<column>. """
        table = self._get_table(name)
        if '.' in name:
            _, name = name.split('.', 1)

        if name not in table.columns:
            raise SpecException("Invalid column: %s" % name)
        return table.columns[name]

    def _scan_columns(self, obj):
        """ Find out which columns are accessed by a particular output
        mapping. """
        columns = []
        if isinstance(obj, Mapping):
            if 'columns' in obj:
                columns.extend(obj['columns'])
            if 'column' in obj:
                columns.append(obj['column'])
            if 'mapping' in obj:
                for o in obj['mapping'].values():
                    columns.extend(self._scan_columns(o))
        else:
            for o in obj:
                columns.extend(self._scan_columns(o))
        return set(columns)

    def _query(self, tables, columns):
        """ Generate a query and iterate over the result cursor. This will
        automatically apply any necessary joins. """
        q = select(columns=columns, from_obj=tables)
        for (left, right) in self.joins:
            if left.table in tables and right.table in tables:
                q = q.where(left == right)

        log.info("Query: %s", q)
        # TODO: see if this scales (i.e. the cursor loads data progressively)
        # else introduce pagination and sorting.
        rp = self.config.ods.execute(q)
        while True:
            rows = rp.fetchmany(10000)
            if not len(rows):
                break
            for row in rows:
                yield dict(row.items())

    def generate(self, mapping_name):
        """ Generate all the items produced by the given mapping. """
        mapping = self.config.get_mapping(mapping_name)
        columns = set(self.get_column(c) for c in self._scan_columns(mapping))
        tables = set([c.table for c in columns])

        _columns = []
        for column in columns:
            alias = '%s.%s' % (column.table.name, column.name)
            _columns.append(column.label(alias))
        return self._query(tables, _columns)
