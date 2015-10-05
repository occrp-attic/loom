import typecast

TYPE_TYPE = 'type'
TYPE_LINK = 'link'


def triplify_object(visitor, data, parent):
    """ Create bi-directional visitors for object relationships. """
    subject = visitor.get_subject(data)
    if visitor.path:
        yield (subject, TYPE_TYPE, visitor.path, TYPE_TYPE)

    if parent is not None:
        yield (parent, visitor.predicate, subject, TYPE_LINK)
        if visitor.reverse is not None:
            yield (subject, visitor.reverse, parent, TYPE_LINK)

    for prop in visitor.properties:
        for res in triplify(prop, data.get(prop.name), subject):
            yield res


def triplify(visitor, data, parent):
    """ Recursively generate statements from the data and schema supplied. """
    if data is None:
        return

    if visitor.is_object:
        for res in triplify_object(visitor, data, parent):
            yield res
    elif visitor.is_array:
        for item in data:
            for res in triplify(visitor.items, item, parent):
                yield res
    else:
        # TODO: figure out if I ever want to check for reverse here.
        type_name = typecast.name(data)
        obj = typecast.stringify(type_name, data)
        yield (parent, visitor.predicate, obj, type_name)
