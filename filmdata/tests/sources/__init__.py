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

    def setUpMixin(self):
        source = __import__('filmdata.sources.%s' % self._name,
                            None, None, ['Produce'])
        self._produce = source.Produce

    def test_produce_data(self):
        for title, data in self._produce.produce_data(('film')):
            self.assertEqual(len(title), 3)
            for key in ('type', 'year', 'name'):
                self.assertTrue(key in title)
                self.assertFalse(title[key] is None)
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0], self._name)
            self.assertTrue('rating' in data[1])
            self.assertFalse(data[1]['rating'] is None)

if __name__ == '__main__':
    unittest.main()
