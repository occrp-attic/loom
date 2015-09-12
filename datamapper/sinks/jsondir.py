from datamapper.sinks.base import Sink


class JSONDirectorySink(Sink):
    """ Store the emitted records as JSON files in a directory
    structure. """
