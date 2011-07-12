from ConfigParser import ConfigParser
import logging.config
import os

from filmdata.lib.dotdict import dotdict

raw_config = ConfigParser()
if os.path.exists('config.ini'):
    raw_config.read('config.ini')
    logging.config.fileConfig('config.ini')

sink = None

config = dotdict()
for section in raw_config.sections():
    if section[:10] != 'formatter_':
        config[section] = dotdict(raw_config.items(section))

config['TITLE_TYPES'] = ('film', 'tv')
config['ROLE_TYPES'] = ('director', 'actor', 'actress', 'producer', 'writer')
config['sources'] = config.core.active_sources.split()
config['role_types'] = config.core.active_role_types.split()
config['role_groups'] = config.core.active_role_groups.split()
config['cast_roles'] = set(config['role_types']) & set(('actor', 'actress'))

config['genre_to_bit_map'] = {
    'drama' : 0,
    'comedy' : 1,
    'short' : 2,
    'foreign' : 3,
    'horror' : 4,
    'documentary' : 5,
    'action' : 6,
    'adventure' : 7,
    'thriller' : 8,
    'romance' : 9,
    'crime' : 10,
    'family' : 11,
    'sci-fi' : 12,
    'fantasy' : 13,
    'mystery' : 14,
    'musical' : 15,
    'war' : 16,
    'western' : 17,
    'indie' : 18,
}
