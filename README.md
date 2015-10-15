# Loom

``loom`` is a command-line tool that maps data from its source table structure
to a common data model, defined through [JSON Schema](http://json-schema.org/).
Once data has been modeled into such objects, it is stored as a set of
statements in a SQL database and eventually indexed in ElasticSearch.

## Configuration

All of ``loom`` is controlled via a configuration file which defines the source
and target databases for data mapping, how to query the source data, available
data types, source details and, most importantly, a mapping between source data
structure and the JSON schema-defined model.

```yaml
ods_database: postgresql://localhost/source_database
loom_database: postgresql://localhost/statements

schemas:
    company: http://schema.occrp.org/generic/company.json#

source:
    slug: foo_companies
    title: "Foo Country Company Registry"
    url: http://registry.gov.foo/companies
tables:
    - fo_companies_company
    - fo_companies_director
joins:
    - foo_companies_company.id: fo_companies_director.company_id
outputs:
    demo:
        schema:
            $ref: http://schema.occrp.org/generic/company.json#
        mapping:
            name:
                column: foo_companies_company.name
                transforms:
                    - clean
                    - latinize
            company_id:
                column: foo_companies_company.id
                format: 'urn:foo:%s'
            directors:
                mapping:
                    name:
                        column: fo_companies_director.name
```
