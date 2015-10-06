
def objectify(load, node, visitor, depth, path=None):
    """ Given a node ID (and it's associated schema), return an
    object the information available about this node. """
    if path is None:
        path = set()
    if visitor.is_object:
        obj = {'$schema': visitor.path, '$sources': []}
        for (p, o, src) in load(node):
            prop = visitor.get_property(p)
            if prop is None or depth <= 1 or o in path:
                continue
            # This is slightly odd but yields purty objects:
            if depth <= 2 and (prop.is_array or prop.is_object):
                continue
            sub_path = path.union([node])

            if src not in obj['$sources']:
                obj['$sources'].append(src)

            value = objectify(load, o, prop, depth - 1, sub_path)
            if prop.is_array and prop.name in obj:
                obj[prop.name].extend(value)
            else:
                obj[prop.name] = value
        return obj
    elif visitor.is_array:
        return [objectify(load, node, visitor.items, depth, path)]
    else:
        return node
