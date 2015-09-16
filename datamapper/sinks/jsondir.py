import os
import json
import shutil

from datamapper.sinks.base import Sink
from datamapper.util import ConfigException


class JSONDirectorySink(Sink):
    """ Store the emitted records as JSON files in a directory
    structure. """

    def get_path(self, source, record=None):
        path = self.config.get('jsondir_path')
        if path is None:
            raise ConfigException("No 'jsondir_path' is configured.")
        path = os.path.join(path, source.slug)
        if record is not None:
            path = os.path.join(path, '%s.json' % record.id)
        return path

    def load(self):
        for record in self.records():
            path = self.get_path(record.source, record=record)
            try:
                os.makedirs(os.path.dirname(path))
            except:
                pass
            with open(path, 'w') as fh:
                json.dump(record.to_dict(), fh)
