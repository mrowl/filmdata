import unittest, os, logging
from nose.tools import nottest
from nose.plugins.skip import SkipTest

from filmdata import config

log = logging.getLogger(__name__)

def only_master(meth):
    def test_only_master_wrapper(self):
        if config.get('core', 'master_source') != self._name:
            raise SkipTest
        return meth(self)
    return test_only_master_wrapper

class FetchMixin(object):
    __test__ = config.get('test', 'fetch').lower() == 'true'

    def setUpMixin(self):
        self._test_dir = config.get('test', 'test_data_dir')
        config.set('DEFAULT', 'data_dir', self._test_dir)
        source = __import__('filmdata.sources.%s' % self._name,
                            None, None, ['Fetch'])
        self._fetch = source.Fetch

    def test_fetch_data(self):
        self._fetch.fetch_data()

    @only_master
    def test_fetch_roles(self):
        for role in ('actor', 'actress', 'director'):
            self._fetch.fetch_roles((role,))

    @only_master
    def test_fetch_aka_titles(self):
        self._fetch.fetch_aka_titles()

    def tearDown(self):
        os.system('rm -r %s' % self._test_dir)

class ProduceMixin(object):

    def setUp(self):
        source = __import__('filmdata.sources.%s' % self._name,
                            None, None, ['Produce'])
        self._produce = source.Produce

if __name__ == '__main__':
    unittest.main()
