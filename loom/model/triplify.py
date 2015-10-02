import typecast

TYPE_TYPE = 'type'
TYPE_LINK = 'link'


def triplify_object(binding):
    """ Create bi-directional bindings for object relationships. """
    if binding.path:
        yield (binding.subject, TYPE_TYPE, binding.path, TYPE_TYPE)

    if binding.parent is not None:
        parent = binding.parent.subject
        if binding.parent.is_array:
            parent = binding.parent.parent.subject
        yield (parent, binding.predicate, binding.subject, TYPE_LINK)
        if binding.reverse is not None:
            yield (binding.subject, binding.reverse, parent, TYPE_LINK)

    for prop in binding.properties:
        for res in triplify(prop):
            yield res


def triplify(binding):
    """ Recursively generate statements from the data and schema supplied. """
    if binding.data is None:
        return

    if binding.is_object:
        for res in triplify_object(binding):
            yield res
    elif binding.is_array:
        for item in binding.items:
            for res in triplify(item):
                yield res
    else:
        # TODO: figure out if I ever want to check for reverse here.
        subject = binding.parent.subject
        type_name = typecast.name(binding.data)
        obj = typecast.stringify(type_name, binding.data)
        yield (subject, binding.predicate, obj, type_name)
