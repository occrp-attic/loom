import logging

from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql.expression import select
from jsonschema import RefResolver
from jsonmapping import Mapper

from datamapper.util import SpecException

log = logging.getLogger(__name__)


class Generator(object):
    """ Apply a mapping specification to generate JSON schema data from a
    SQL database using field mappings. """

    def __init__(self, config, spec):
        self.config = config
        self.spec = spec

    @property
    def metadata(self):
        if not hasattr(self, '_metadata'):
            self._metadata = MetaData()
            self._metadata.bind = self.config.engine
        return self._metadata

    @property
    def tables(self):
        if not hasattr(self, '_tables'):
            self._tables = []
            for table_name in self.spec.get('tables', []):
                table = Table(table_name, self.metadata, autoload=True)
                self._tables.append(table)
        return self._tables

    @property
    def joins(self):
        if not hasattr(self, '_joins'):
            self._joins = []
            for join in self.spec.get('joins', {}):
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
        columns = set()
        if 'column' in obj:
            columns.add(obj['column'])
        if 'columns' in obj:
            columns = columns.union(obj['columns'])
        if 'mapping' in obj:
            for o in obj['mapping'].values():
                columns = columns.union(self._scan_columns(o))
        return columns

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
        rp = self.config.engine.execute(q)
        while True:
            row = rp.fetchone()
            if row is None:
                break
            yield dict(row.items())

    def generate(self, mapping_name, full_tables=False):
        """ Generate all the items produced by the given form. """
        mapping = self.spec.get('mappings', {}).get(mapping_name)
        if mapping is None:
            raise SpecException("No such mapping: %s", mapping_name)

        columns = set(self.get_column(c) for c in self._scan_columns(mapping))
        tables = set([c.table for c in columns])
        if full_tables:
            # NOTE: this is intentionally not just selecting the required
            # columns in order to return the full table data.
            for table in tables:
                for column in table.columns:
                    columns.add(column)

        _columns = []
        for column in columns:
            alias = '%s.%s' % (column.table.name, column.name)
            _columns.append(column.label(alias))

        mapper = Mapper(mapping, self.config.resolver,
                        scope=self.config.base_uri)
        for row in self._query(tables, _columns):
            _, data = mapper.apply(row)
            # TODO: perform validation here?
            yield data, row
