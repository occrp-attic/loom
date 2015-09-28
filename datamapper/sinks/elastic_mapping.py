from jsonmapping.elastic import generate_schema_mapping


BASE_MAPPING = {
    "_id": {"path": "id"},
    "_all": {"enabled": True},
    "properties": {
        "id": {"type": "string", "index": "not_analyzed"},
        "schema": {"type": "string", "index": "not_analyzed"},
        "indexed_at": {"type": "date", "index": "not_analyzed"},
        "raw": {
            "type": "object",
            "properties": {}
        },
        "source": {"type": "string", "index": "not_analyzed"}
    }
}


def generate_mapping(mapping, index, doc_type, record, resolver):
    """ Generate a mapping. """
    mapping = mapping.get(index, {}).get('mappings', {})
    mapping = mapping.get(doc_type, BASE_MAPPING)

    val = {'type': 'string', 'index': 'analyzed', 'store': True}
    for field in record.raw.keys():
        mapping['properties']['raw']['properties'][field] = val

    entity = generate_schema_mapping(resolver, record.schema)
    mapping['properties']['entity'] = entity
    return mapping
