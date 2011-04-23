from ConfigParser import ConfigParser
import logging.config
import os

if os.path.exists('config.ini'):
    config = ConfigParser()
    config.read('config.ini')

    logging.config.fileConfig('config.ini')
else:
    config = None

sink = None
data_keys = None
