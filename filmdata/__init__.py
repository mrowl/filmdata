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
