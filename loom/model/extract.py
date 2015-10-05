from jsonmapping import Mapper

from loom.model.binding import Binding
from loom.model.triplify import triplify


class Extractor(Mapper):

    def __init__(self, mapping, resolver, scope):
        super(Extractor, self).__init__(mapping, resolver, scope=scope)

    def extract(self, row):
        _, data = self.apply(row)
        binding = Binding(self.bind.schema, self.resolver, scope=self.scope,
                          data=data)
        for stmt in triplify(binding):
            yield stmt
