from pkg_resources import iter_entry_points


class Sink(object):
    """ A sink will receive a generator and attempt to store the emitted
    records in a backend (e.g. search index, files, database). """

    def __init__(self, config, generator):
        self.config = config
        self.generator = generator

    def records(self):
        for mapping in self.generator.mappings:
            for record in self.generator.generate(mapping, full_tables=True):
                yield record

    def load(self):
        raise NotImplementedError()

    def clear(self):
        raise NotImplementedError()

    @classmethod
    def by_name(cls, name):
        for ep in iter_entry_points('datamapper.sinks'):
            if ep.name == name:
                return ep.load()
