import os, pkgutil

from filmdata import config

class PluginNotFoundException(Exception): pass

class PluginManager(object):

    def __init__(self, pkg_name, filename, objects=('*')):
        plugins_path = [os.path.dirname(filename)]
        self.list = [name for _, name, _ in pkgutil.iter_modules(plugins_path)]
        self._pkg_name = pkg_name
        self._objects = objects

    def load(self, name, objects=None):
        if objects == None:
            objects = self._objects
        if not name in self.list:
            raise PluginNotFoundException("%s plugin %s not found" % (self._pkg_name, name))
        plugin = __import__('%s.%s' % (self._pkg_name, name),
                            None, None, objects)
        return plugin

    def iter(self, objects=None):
        for name in self.list:
            yield (name, self.load(name, objects))
