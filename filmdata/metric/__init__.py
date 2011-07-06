import filmdata.sink
from filmdata import config
from filmdata.lib.plugin_manager import PluginManager

class Metric:
    
    @property
    def stats(self):
        if not hasattr(self, '_stats'):
            self._scipy = __import__('scipy', None, None, ['stats'])
            self._stats = self._scipy.stats
        return self._stats

    @property
    def numpy(self):
        if not hasattr(self, '_numpy'):
            self._numpy = __import__('numpy')
        return self._numpy

    def __init__(self):
        self._mean_field = 'mean'
        self._count_field = 'count'
        self._min_votes = {
            'imdb' : 3500,
            'netflix' : 10000,
        }
        self._count_sources = ('imdb', 'netflix')
        self._sink = filmdata.sink
        self._sources = config.core.active_sources.split()
        self._cull_source = config.core.primary_data

    def __call__(self):
        pass

manager = PluginManager('filmdata.metric', __file__, parent_class=Metric)
