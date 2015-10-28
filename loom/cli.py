import logging

import click

from loom.util import LoomException, load_config
from loom.config import Config
from loom.spec import Spec
from loom.mapper import Mapper
from loom.indexer import Indexer

log = logging.getLogger(__name__)


@click.group()
@click.option('--debug/--no-debug', default=False,
              help='Show log messages.')
@click.option('config_file', '--config', '-c', envvar='LOOM_CONFIG',
              required=True, type=click.Path(),
              help='Configuration file.')
@click.pass_context
def cli(ctx, debug, config_file):
    """ Map data from a SQL database into a statement-based graph. """
    ctx.obj = ctx.obj or {}
    ctx.obj['DEBUG'] = debug

    config = load_config(config_file)
    ctx.obj['CONFIG'] = Config(config, path=config_file)

    fmt = '[%(levelname)-8s] %(name)-12s: %(message)s'
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=fmt)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)


@cli.command('map')
@click.argument('spec_file', type=click.Path(exists=True))
@click.pass_context
def map(ctx, spec_file):
    """ Map data from the database into modeled objects. """
    try:
        config = ctx.obj['CONFIG']
        spec = load_config(spec_file)
        spec = Spec(config, spec, path=spec_file)

        log.info("Registering source: %r", spec.source)
        config.sources.upsert(spec.get('source', {}))

        mapper = Mapper(config, spec)
        mapper.map()
    except LoomException as le:
        raise click.ClickException(le.message)


@cli.command('index')
@click.option('schema', '-t', '--schema', default=None,
              help='Index only entities of the given type')
@click.option('source', '-s', '--source', default=None,
              help='Index only entities from the given source')
@click.pass_context
def index(ctx, schema, source):
    """ Index modeled objects to ElasticSearch. """
    try:
        config = ctx.obj['CONFIG']
        indexer = Indexer(config)
        indexer.index(schema=schema, source=source)
    except LoomException as le:
        raise click.ClickException(le.message)


@cli.command('flush')
@click.option('source', '-s', '--source', default=None,
              help='Delete only entities from the given source')
@click.option('flush_all', '-a', '--all', default=False,
              help='Delete all entities')
@click.pass_context
def flush(ctx, source, flush_all):
    """ Clear all statements about a source from the statement DB. """
    try:
        if source is None and not flush_all:
            msg = "Either specify a --source or pass --all"
            raise click.ClickException(msg)
        config = ctx.obj['CONFIG']
        config.entities.delete(source)
        config.properties.delete(source)
    except LoomException as le:
        raise click.ClickException(le.message)


@cli.command('dedupe')
@click.pass_context
def dedupe(ctx):
    """ De-duplicate statements inside the statement DB. """
    try:
        config = ctx.obj['CONFIG']
        config.entities.dedupe()
        config.properties.dedupe()
    except LoomException as le:
        raise click.ClickException(le.message)


if __name__ == '__main__':
    cli(obj={})
