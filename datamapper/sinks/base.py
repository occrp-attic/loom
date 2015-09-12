

class Sink(object):
    """ A sink will receive a generator and attempt to store the emitted
    records in a backend (e.g. search index, files, database). """

    def __init__(self, config):
        self.config = config

    def load(self, generator):
        raise NotImplemented()

    def clear(self, generator):
        raise NotImplemented()
