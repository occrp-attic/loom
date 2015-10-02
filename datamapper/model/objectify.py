
def objectify(load, node, binding, depth, path=None):
    """ Given a node ID (and it's associated schema), return an
    object the information available about this node. """
    if path is None:
        path = set()
    if binding.is_object:
        obj = {'$schema': binding.path, '$sources': []}
        for (p, o, src) in load(node):
            prop = binding.get_property(p)
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
    elif binding.is_array:
        for item in binding.items:
            return [objectify(load, node, item, depth, path)]
    else:
        return node
