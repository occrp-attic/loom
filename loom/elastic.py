from jsonmapping.elastic import generate_schema_mapping


BASE_MAPPING = {
    "_id": {"path": "id"},
    "_all": {"enabled": True},
    "properties": {
        "$schema": {"type": "string", "index": "not_analyzed"},
        "$sources": {"type": "integer", "index": "not_analyzed"},
        "$collections": {"type": "integer", "index": "not_analyzed"},
        "$authors": {"type": "string", "index": "not_analyzed"},
        "$text": {"type": "string", "index": "analyzed"},
        "$linkcount": {"type": "integer", "index": "not_analyzed"},
        "$attrcount": {"type": "integer", "index": "not_analyzed"},
        "$latin": {"type": "string", "index": "analyzed"},
        "$suggest": {"type": "string", "analyzer": "autocomplete"},
        "$indexed_at": {"type": "date", "index": "not_analyzed"}
    }
}

BASE_SETTINGS = {
    "analysis": {
        "analyzer": {
            "autocomplete": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["standard", "lowercase", "autocomp"]
            }
        },
        "filter": {
            "autocomp": {
                "type": "edge_ngram",
                "min_gram": 1,
                "max_gram": 20
            }
        }
    }
}


def generate_mapping(schema, resolver):
    """ Generate a mapping. """
    mapping = BASE_MAPPING
    schema_mapping = generate_schema_mapping(resolver, schema, depth=2)
    mapping['properties'].update(schema_mapping.get('properties'))
    return mapping
