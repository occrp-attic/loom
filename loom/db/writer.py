import logging
import hashlib
from time import time
from threading import Thread
from Queue import Queue

from bson.objectid import ObjectId

log = logging.getLogger(__name__)


class Writer(object):
    """ Do chunked bulk writes to the database, against a particular table
    manager. """

    def __init__(self, manager):
        self.manager = manager
        self.chunk = 100000
        self.queue = Queue(maxsize=self.chunk * 2)
        self.thread = Thread(target=self.loader_thread)
        self.thread.daemon = True
        self.thread.start()
        self.buffer = []

    def loader_thread(self):
        while True:
            record = self.queue.get()
            self.buffer.append(record)
            if len(self.buffer) > self.chunk:
                self.clear()
            self.queue.task_done()

    def clear(self):
        if len(self.buffer):
            log.info("Flushing %r buffer...", self.manager.name)
            bulk = self.manager.collection.initialize_unordered_bulk_op()
            for record in self.buffer:
                pattern = {'_id': record['_id']}
                bulk.find(pattern).upsert().replace_one(record)
            bulk.execute()
            self.buffer = []

    def flush(self):
        self.queue.join()
        self.clear()

    def write(self, record):
        sha = hashlib.sha1()
        for field in self.manager.unique:
            sha.update(record[field].encode('utf-8'))
        record['_id'] = ObjectId(sha.hexdigest()[:24])
        self.queue.put(record)
