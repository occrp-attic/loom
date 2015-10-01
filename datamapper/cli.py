import logging

import yaml
import click

from datamapper.util import DataMapperException
from datamapper.config import Config
from datamapper.mapper import Mapper
from datamapper.indexer import Indexer

log = logging.getLogger(__name__)


@click.group()
@click.option('--debug/--no-debug', default=False,
              help='Show log messages.')
@click.option('--db', default=None,
              help='A database URI.')
@click.option('--config', '-c', required=True, type=click.Path(exists=True),
              help='A configuration file.')
@click.pass_context
def cli(ctx, debug, db, config):
    """ Map data from a SQL database into a variety of data sinks. """
    ctx.obj = ctx.obj or {}
    ctx.obj['DEBUG'] = debug

    fmt = '[%(levelname)-8s] %(name)-12s: %(message)s'
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=fmt)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)

    ctx.obj['CONFIG'] = Config.from_path(config, database=db)


@cli.command('map')
@click.argument('model_file', type=click.File('r'))
@click.pass_context
def map(ctx, model_file):
    """ Map data from the database into modeled objects. """
    try:
        config = ctx.obj['CONFIG']
        model = yaml.load(model_file)
        mapper = Mapper(config, model)
        mapper.map()
    except DataMapperException as dme:
        raise click.ClickException(dme.message)


@cli.command('index')
@click.argument('model_file', type=click.File('r'))
@click.pass_context
def index(ctx, model_file):
    """ Index modeled objects to ElasticSearch. """
    try:
        config = ctx.obj['CONFIG']
        model = yaml.load(model_file)
        indexer = Indexer(config, model)
        indexer.index()
    except DataMapperException as dme:
        raise click.ClickException(dme.message)

if __name__ == '__main__':
    cli(obj={})
