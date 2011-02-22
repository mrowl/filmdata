from ConfigParser import ConfigParser
import logging.config

config = ConfigParser()
config.read('config.ini')


logging.config.fileConfig('config.ini')
#logger = logging.getLogger("filmdata")

#sh = logging.StreamHandler()
#sh.setLevel(logging.INFO)

#sh.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
#logger.addHandler(sh)
