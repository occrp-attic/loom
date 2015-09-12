from pkg_resources import iter_entry_points


def get_sink(name):
    for ep in iter_entry_points('datamapper.sinks'):
        if ep.name == name:
            return ep.load()
