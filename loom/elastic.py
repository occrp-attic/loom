from jsonmapping.elastic import generate_schema_mapping


BASE_MAPPING = {
    "_id": {"path": "id"},
    "_all": {"enabled": True},
    "properties": {
        "$schema": {"type": "string", "index": "not_analyzed"},
        "$sources": {"type": "string", "index": "not_analyzed"},
        "$text": {"type": "string", "index": "analyzed"},
        "$linkcount": {"type": "integer", "index": "not_analyzed"},
        "$attrcount": {"type": "integer", "index": "not_analyzed"},
        "$latin": {"type": "string", "index": "analyzed"},
        "$indexed_at": {"type": "date", "index": "not_analyzed"}
    }
}

# SETTINGS = {
#     "index": {
#         "analysis": {
#             "analyzer": {
#                 "latin": {
#                     "tokenizer": "icu_tokenizer",
#                     "filter": ["latinize", "lowercase"]
#                 }
#             },
#             "filter": {
#                 "latinize": {
#                     "type": "icu_transform",
#                     "id": "Any-Latin; NFD; [:Nonspacing Mark:] Remove; NFC; Latin-ASCII"
#                 }
#             }
#         }
#     }
# }


def generate_mapping(index, doc_type, schema, resolver):
    """ Generate a mapping. """
    mapping = BASE_MAPPING
    schema_mapping = generate_schema_mapping(resolver, schema)
    mapping['properties'].update(schema_mapping.get('properties'))
    return mapping
