import os
import json
import shutil
import logging
from jsongraph import Graph

from datamapper.sinks.base import Sink
from datamapper.util import ConfigException

log = logging.getLogger(__name__)


class NQuadsSink(Sink):
    """ Generate NQuad files from the incoming records. """

    def make_context(self, record):
        graph = Graph(self.config.base_uri, resolver=self.config.resolver)
        graph.register(self.config.get_alias(record.schema), record.schema)
        for alias, schema in self.config.schemas.items():
            graph.register(alias, schema)
        return graph.context(meta=record.source.to_dict())

    def get_path(self, record, section):
        path = self.config.get('nquads_path')
        if path is None:
            raise ConfigException("No 'nquads_path' is configured.")
        doc_type = self.config.get_alias(record.schema)
        file_name = '%s.%s.nq' % (doc_type, section)
        return os.path.join(path, record.source.slug, file_name)

    def store(self, context, record, section):
        path = self.get_path(record, section)
        try:
            os.makedirs(os.path.dirname(path))
        except:
            pass
        context.save()
        log.info("Storing data to: %s", path)
        with open(path, 'w') as fh:
            graph = context.parent
            graph.graph.serialize(fh, format='nquads')

    def load(self):
        section = 1
        for mapping in self.generator.mappings:
            context = None
            for i, record in enumerate(self.generator.generate(mapping)):
                if context is None:
                    context = self.make_context(record)
                doc_type = self.config.get_alias(record.schema)
                context.add(doc_type, record.entity)
                if i > 0 and i % 10000 == 0:
                    log.info("Generaring %r NQuads: %s records", doc_type, i)
                if i > 0 and i % 100000 == 0:
                    self.store(context, record, section)
                    context = None
                    section += 1
            self.store(context, record, section)
