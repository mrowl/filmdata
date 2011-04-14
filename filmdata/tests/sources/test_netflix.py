import unittest

import filmdata.tests.sources as mixins

class TestNetflixFetch(mixins.FetchMixin, unittest.TestCase):

    def setUp(self):
        self._name = 'netflix'
        self.setUpMixin()

if __name__ == '__main__':
    unittest.main()
