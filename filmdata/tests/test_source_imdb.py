import unittest, os, logging
from nose.tools import nottest

from filmdata import config
from filmdata.sources.imdb import ImdbSource

log = logging.getLogger(__name__)

class TestImdbFetch(unittest.TestCase):
    __test__ = config.get('test', 'fetch').lower() == 'true'

    def setUp(self):
        config.set('DEFAULT', 'data_dir', 'data_test')
        self.source = ImdbSource()

    def test_fetch_data(self):
        self.source.fetch_data()
        assert os.access(config.get('imdb', 'rating_path'), os.R_OK)
        assert not os.path.isfile(config.get('imdb', 'rating_path') + '.gz')

    def test_fetch_aka_titles(self):
        self.source.fetch_aka_titles()
        assert os.access(config.get('imdb', 'aka_path'), os.R_OK)
        assert not os.path.isfile(config.get('imdb', 'aka_path') + '.gz')

    @nottest
    def test_fetch_role(self, role):
        self.source.fetch_roles((role,))
        assert os.access(config.get('imdb', '%s_path' % role), os.R_OK)
        assert not os.path.isfile(config.get('imdb', '%s_path' % role) + '.gz')

    def test_fetch_actor(self):
        self.test_fetch_role('actor')

    def test_fetch_actress(self):
        self.test_fetch_role('actor')

    def test_fetch_director(self):
        self.test_fetch_role('actor')

    def tearDown(self):
        os.system('rm -r %s' % config.get('DEFAULT', 'data_dir'))
        del self.source

if __name__ == '__main__':
    unittest.main()
