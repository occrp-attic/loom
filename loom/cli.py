import logging

import click

from loom.util import LoomException, load_config
from loom.config import Config
from loom.mapper import Mapper
from loom.indexer import Indexer

log = logging.getLogger(__name__)


@click.group()
@click.option('--debug/--no-debug', default=False,
              help='Show log messages.')
@click.pass_context
def cli(ctx, debug):
    """ Map data from a SQL database into a statement-based graph. """
    ctx.obj = ctx.obj or {}
    ctx.obj['DEBUG'] = debug

    fmt = '[%(levelname)-8s] %(name)-12s: %(message)s'
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=fmt)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)


@cli.command('init')
@click.argument('config_file', type=click.Path(exists=True))
@click.pass_context
def init(ctx, config_file):
    """ Register a source from a configuration file with the loom DB. """
    try:
        config = load_config(config_file)
        config = Config(config, path=config_file)

        log.info("Registering source: %r", config.source)
        config.sources.upsert(config.get('source', {}))
    except LoomException as le:
        raise click.ClickException(le.message)


@cli.command('map')
@click.argument('config_file', type=click.Path(exists=True))
@click.pass_context
def map(ctx, config_file):
    """ Map data from the database into modeled objects. """
    try:
        config = load_config(config_file)
        config = Config(config, path=config_file)

        log.info("Registering source: %r", config.source)
        config.sources.upsert(config.get('source', {}))

        mapper = Mapper(config)
        mapper.map()
    except LoomException as le:
        raise click.ClickException(le.message)


@cli.command('index')
@click.argument('config_file', type=click.Path(exists=True))
@click.pass_context
def index(ctx, config_file):
    """ Index modeled objects to ElasticSearch. """
    try:
        config = load_config(config_file)
        config = Config(config, path=config_file)
        indexer = Indexer(config)
        indexer.configure()
        indexer.index()
    except LoomException as le:
        raise click.ClickException(le.message)


@cli.command('flush')
@click.argument('config_file', type=click.Path(exists=True))
@click.pass_context
def flush(ctx, config_file):
    """ Clear all statements about a source from the statement DB. """
    try:
        config = load_config(config_file)
        config = Config(config, path=config_file)
        config.entities.delete(config.source)
        config.properties.delete(config.source)
    except LoomException as le:
        raise click.ClickException(le.message)


@cli.command('dedupe')
@click.argument('config_file', type=click.Path(exists=True))
@click.pass_context
def dedupe(ctx, config_file):
    """ De-duplicate statements inside the statement DB. """
    try:
        config = load_config(config_file)
        config = Config(config, path=config_file)
        config.entities.dedupe(config.source)
        config.properties.dedupe(config.source)
    except LoomException as le:
        raise click.ClickException(le.message)


if __name__ == '__main__':
    cli(obj={})
