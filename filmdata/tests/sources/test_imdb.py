import unittest

import filmdata.tests.sources as mixins

class TestImdbFetch(mixins.FetchMixin, unittest.TestCase):

    def setUp(self):
        self._name = 'imdb'
        self.setUpMixin()

if __name__ == '__main__':
    unittest.main()
