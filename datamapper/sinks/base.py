from pkg_resources import iter_entry_points


class Sink(object):
    """ A sink will receive a generator and attempt to store the emitted
    records in a backend (e.g. search index, files, database). """

    def __init__(self, config, generator):
        self.config = config
        self.generator = generator

    def load(self):
        raise NotImplementedError()

    def clear(self):
        raise NotImplementedError()

    @classmethod
    def by_name(cls, name):
        for ep in iter_entry_points('datamapper.sinks'):
            if ep.name == name:
                return ep.load()
