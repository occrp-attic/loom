# Loom

``loom`` is a command-line tool that maps data from its source table structure
to a common data model, defined through [JSON Schema](http://json-schema.org/).
Once data has been modeled into such objects, it is stored as a set of
statements in a SQL database and eventually indexed in ElasticSearch.

## Design

The design goal of ``loom`` is to accept data from many different sources and
in many different formats, and to integrate it into a single, coherent data
model that can be used for analysis.

Imagine, for example, trying to integrate data about a senior politician. You
might find data about that person on WikiData, by scraping a parliament's web
site, extracting data from expense reports and by looking up company ownership
records in a corporate registry.

The purpose of ``loom`` is to read data from all of these sources, and to
translate each source record into a (partially-filled) JSON object representing
a person. This object may have nested items, such as information about party
posts, company directorships etc.

``loom`` will also provide means for de-duplicating entities, so that all the
different records about the politician coming from various sources will be
merged into a single, coherent and complete profile. This is made possible by
splitting the information into atomic statements (often called triples or quads,
[learn more](http://www.w3.org/TR/rdf11-concepts/#section-triples)).

Using statements, the information can also be re-constructed in different ways
than it was originally submitted. For example, after importing a list of people
with nested information about what companies they control, you could very
easily invert that into a list of companies with nested information about the
people who control them.

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

## Installation

If you wish to use the ``loom`` command line tool, you can install the
application into a Python [virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
like this:

```bash
$ pip install git+https://github.com/occrp/loom
```

To do development on the tool, you should instead check out a local copy:

```bash
$ git clone git@github.com:occrp/loom.git
$ cd loom
$ make install
$ make test
```

Instead of executing the ``Makefile`` commands (which create a virtual
environment and install the necessary dependencies), you can also run these
steps manually.

## Similar work and references

``loom`` is heavily inspired by Linked Data and RDF. If you're interested in
similar tools in that ecosystem, check out:

* [Grafter](http://grafter.org/)
* [Silk Framework](http://silk-framework.com/)

## License

``loom`` is free software; it is distributed under the terms of the Affero
General Public License, version 3.
