

class Writer(object):
    """ Do chunked bulk writes to the database, against a particular table
    manager. """

    def __init__(self, manager, conn, chunk_size=10000):
        self.manager = manager
        self.conn = conn
        self.chunk_size = chunk_size
        self.buffer = []

    def flush(self):
        if len(self.buffer):
            stmt = self.manager.table.insert()
            self.conn.execute(stmt, self.buffer)
        self.buffer = []

    def write(self, record):
        self.buffer.append(record)
        if self.chunk_size >= len(self.buffer):
            self.flush()
