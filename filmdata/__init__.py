from ConfigParser import ConfigParser
import logging.config

config = ConfigParser()
config.read('config.ini')

logging.config.fileConfig('config.ini')

sink = None
