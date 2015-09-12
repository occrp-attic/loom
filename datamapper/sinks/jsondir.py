from datamapper.sinks.base import Sink


class JSONDirectorySink(Sink):
    """ Store the emitted records as JSON files in a directory
    structure. """

    def load(self):
        for record in self.records():
            from pprint import pprint
            pprint(record.to_dict())
