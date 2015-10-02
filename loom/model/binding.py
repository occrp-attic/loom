from jsonmapping import SchemaVisitor

from loom.util import make_id


class Binding(SchemaVisitor):

    @property
    def subject(self):
        if not hasattr(self, '_subject'):
            self._subject = None
            subject = self.schema.get('rdfSubject', 'id')
            for prop in self.properties:
                if prop.match(subject):
                    self._subject = prop.data
            if self._subject is None:
                self._subject = make_id()
        return self._subject

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
