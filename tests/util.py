import os
import logging
import unicodecsv
import dataset
from normality import slugify

logging.getLogger('alembic').setLevel(logging.WARN)
logging.getLogger('dataset').setLevel(logging.WARN)

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), 'fixtures')
SHARED = {}

def create_fixtures():
    if 'engine' not in SHARED:
        conn = dataset.connect('sqlite://')
        for table in ['companies', 'financials']:
            with open(os.path.join(FIXTURE_PATH, table + '.csv'), 'r') as fh:
                for row in unicodecsv.DictReader(fh):
                    data = {slugify(k, sep='_'): v for k, v in row.items()}
                    conn[table].insert(data)
        SHARED['engine'] = conn.engine
    return SHARED['engine']
