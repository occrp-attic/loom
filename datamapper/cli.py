import logging

import yaml
import click

from datamapper.config import Config

log = logging.getLogger(__name__)


@click.group()
@click.option('--debug/--no-debug', default=False,
              help='Show log messages.')
@click.option('--config', '-c', required=True, type=click.Path(exists=True),
              help='A configuration file.')
@click.pass_context
def cli(ctx, debug, config):
    """ Map data from a SQL database into a variety of data sinks. """
    ctx.obj = ctx.obj or {}
    ctx.obj['DEBUG'] = debug

    fmt = '[%(levelname)-8s] %(name)-12s: %(message)s'
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=fmt)
    logging.getLogger('requests').setLevel(logging.WARNING)

    ctx.obj['CONFIG'] = Config.from_path(config)


@cli.command('load')
@click.argument('spec_file', type=click.File('r'))
@click.argument('sink')
@click.pass_context
def load(ctx, raw, spec_file, sink):
    """ Load data from the database into a sink. """
    config = ctx.obj['CONFIG']
    spec = yaml.load(spec_file)


@cli.command('clear')
@click.argument('spec_file', type=click.File('r'))
@click.argument('sink')
@click.pass_context
def clear(ctx, spec_file, sink):
    """ Delete all data from a given spec in a sink. """
    config = ctx.obj['CONFIG']
    spec = yaml.load(spec_file)


if __name__ == '__main__':
    cli(obj={})
