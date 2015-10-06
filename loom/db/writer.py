import logging
import unicodecsv
import tempfile
from time import time

log = logging.getLogger(__name__)


class Writer(object):
    """ Do chunked bulk writes to the database, against a particular table
    manager. """

    def __init__(self, manager, engine):
        self.manager = manager
        self.engine = engine
        self.temp = tempfile.NamedTemporaryFile(suffix='.csv')
        self.fields = [unicode(c.name) for c in self.manager.columns]
        self.fields = [f for f in self.fields if f != 'id']
        self.writer = unicodecsv.DictWriter(self.temp, fieldnames=self.fields)
        self.rows = 0

    def flush(self):
        self.temp.flush()
        self.temp.seek(0)
        begin = time()
        raw_conn = self.engine.raw_connection()
        log.info("Bulk loading %s rows into %r", self.rows, self.manager.name)
        cur = raw_conn.cursor()
        q = """
            COPY %s (%s) FROM STDIN
                WITH CSV HEADER DELIMITER ',' QUOTE '\"'
                ENCODING 'utf-8'
        """ % (self.manager.name, ', '.join(self.fields))
        cur.copy_expert(q, self.temp)
        raw_conn.commit()
        cur.close()
        duration = (time() - begin)
        log.info("COPY done after %.2fs", duration)
        self.temp.close()

    def write(self, record):
        self.writer.writerow(record)
        self.rows += 1
