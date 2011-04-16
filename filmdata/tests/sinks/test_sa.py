import unittest

from filmdata.sinks.sa.base import SaSink as Sink

class TestSinkSa(unittest.TestCase):

    def setUp(self):
        self._sink = Sink()

    def test_get_titles_rating(self):
        count = 0
        for t in self._sink.get_titles_rating():
            count += 1
            for k in ('title_id', 'imdb_rating',
                      'imdb_votes', 'netflix_rating'):
                self.assertTrue(k in t)
                self.assertFalse(k is None)
        self.assertTrue(count > 100)
