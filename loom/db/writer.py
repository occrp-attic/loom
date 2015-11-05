import logging
import unicodecsv
import tempfile
from time import time

from threading import Thread
from Queue import Queue

log = logging.getLogger(__name__)

IGNORE = ['id', 'collection_id', 'author', 'created_at']


class Writer(object):
    """ Do chunked bulk writes to the database, against a particular table
    manager. """

    def __init__(self, manager):
        self.manager = manager
        self.engine = manager.config.engine
        self.fields = [unicode(c.name) for c in self.manager.table.columns]
        self.fields = [f for f in self.fields if f not in IGNORE]
        self.rows = 0
        self.create_file()

        self.queue = Queue(maxsize=2)
        self.thread = Thread(target=self.loader_thread)
        self.thread.daemon = True
        self.thread.start()

    def create_file(self):
        self.temp = tempfile.NamedTemporaryFile(suffix='.csv')
        self.writer = unicodecsv.DictWriter(self.temp, fieldnames=self.fields)

    def loader_thread(self):
        while True:
            rows, temp = self.queue.get()
            self.bulk_load(rows, temp)
            self.queue.task_done()

    def bulk_load(self, rows, temp):
        temp.flush()
        temp.seek(0)
        begin = time()
        raw_conn = self.engine.raw_connection()
        log.info("Bulk loading into %r (ca. %s)", self.manager.name, rows)
        cur = raw_conn.cursor()
        q = """
            COPY %s (%s) FROM STDIN
                WITH CSV HEADER DELIMITER ',' QUOTE '\"'
                ENCODING 'utf-8'
        """ % (self.manager.name, ', '.join(self.fields))
        cur.copy_expert(q, temp)
        raw_conn.commit()
        cur.close()
        duration = (time() - begin)
        log.info("COPY done after %.2fs", duration)
        temp.close()

    def flush(self):
        if self.manager.config.is_postgresql:
            self.queue.put((int(self.rows), self.temp))
            self.queue.join()

    def write(self, record):
        self.rows += 1
        if self.manager.config.is_postgresql:
            self.writer.writerow(record)
            if self.rows > 0 and self.rows % 500000 == 0:
                self.queue.put((int(self.rows), self.temp))
                self.create_file()
        else:
            self.manager.insert_many([record])
