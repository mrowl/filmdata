import unittest, os, logging
from nose.tools import nottest
from nose.plugins.skip import SkipTest

from filmdata import config
import filmdata.sources

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
        source = filmdata.sources.manager.load(self._name)
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
        source = filmdata.sources.manager.load(self._name)
        self._produce = source.Produce
        self._title_types = ('film',)

    def _check_title_key(self, title):
        self.assertEqual(len(title), 3)
        for key in ('type', 'year', 'name'):
            self.assertTrue(key in title)
            self.assertFalse(title[key] is None)


    def test_produce_data(self):
        for title, data in self._produce.produce_data(self._title_types):
            self._check_title_key(title)
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0], self._name)
            self.assertTrue('rating' in data[1])
            self.assertFalse(data[1]['rating'] is None)
            self.assertTrue(data[1]['rating'] <= config.get('core',
                                                            'max_rating'))
            self.assertTrue(data[1]['rating'] >= 0)
    
    @only_master
    def test_produce_roles(self):
        role_types = ('actor', 'actress', 'director')
        for role in self._produce.produce_roles(self._title_types, role_types):
            self.assertEqual(len(role), 3)
            self.assertFalse(role[0] is None)
            self.assertFalse(role[1] is None)
            self.assertFalse(role[2] is None)
            self._check_title_key(role[0])

            for k in ('type', 'character', 'billing'):
                self.assertTrue(k in role[1])
            self.assertTrue(role[1]['type'] in role_types)

            self.assertTrue('name' in role[2])
            self.assertFalse(role[2]['name'] is None)

    @only_master
    def test_produce_aka_titles(self):
        for title, aka in self._produce.produce_aka_titles(self._title_types):
            self._check_title_key(title)
            for k in ('name', 'year', 'region'):
                self.assertTrue(k in aka)
                self.assertFalse(aka[k] is None)

if __name__ == '__main__':
    unittest.main()
