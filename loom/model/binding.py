from collections import Mapping

from jsonmapping import SchemaVisitor

from loom.util import make_id


class Binding(SchemaVisitor):

    def get_subject(self, data):
        if not isinstance(data, Mapping):
            return None
        subject = self.schema.get('rdfSubject', 'id')
        if data.get(subject):
            return data.get(subject)
        return make_id()

    @property
    def predicate(self):
        return self.schema.get('rdfName', self.name)

    @property
    def reverse(self):
        name = self.schema.get('rdfReverse')
        if name is not None:
            return name
        if self.parent is not None and self.parent.is_array:
            return self.parent.reverse

    def get_property(self, predicate):
        for prop in self.properties:
            if predicate == prop.name:
                return prop
