"""
A very simple plugin manager which basically just manages all the modules in
a package.  Plugin == Module throughout.
"""

import os, pkgutil

class PluginNotFoundException(Exception):
    """Exception to raise when a plugin isn't found"""
    
    pass

class PluginManager(object):
    """
    Collect the modules for a given package and exposes them via:
        1. a list of names (MyPluginManager.list).
        2. iterator which returns the name and actual module object.
    Can also directly load a given module that's in the package.
    Attributes:
        list - a list of module names in the package.

    Example:
        # in filmdata/source/__init__.py
        from filmdata.lib.plugin_manager import PluginManager

        # manages the Fetch and Produce classes and schema object
        # for the source package
        manager = PluginManager('filmdata.source', __file__,
                                ('Fetch', 'Produce', 'schema'))


        # using the manager in some random file to print the schemas
        import filmdata.source
        for name, source filmdata.source.manager.iter():
            print source.schema
    """

    def __init__(self, pkg_name, pkg_path, objects=('*')):
        """
        Create a new plugin manager.
        Arguments:
            pkg_name - the name of the package for which to collect modules.
                e.g. filmdata.source
            pkg_path - the path for the package (usually just pass __file__)
            objects - the names of the particular classes, functions, etc. to
                load for each module in the package when using __import__.
                e.g. ('Fetch', 'Produce', 'schema') for a source
        """
        plugins_path = [os.path.dirname(pkg_path)]
        self.list = [name for _, name, _ in pkgutil.iter_modules(plugins_path)]
        self._pkg_name = pkg_name
        self._objects = objects

    def load(self, name, objects=None):
        """
        Load a specified module.
        Arguments:
            name - the name of a module, e.g. 'netflix'
            objects - see description of similar arg for __init__
        Returns the module object
        """
        if objects == None:
            objects = self._objects
        if not name in self.list:
            raise PluginNotFoundException("%s plugin %s not found" %
                                          (self._pkg_name, name))
        plugin = __import__('%s.%s' % (self._pkg_name, name),
                            None, None, objects)
        return plugin

    def iter(self, objects=None):
        """
        Iterate over all the modules in the package.
        Arguments:
            objects - see above
        Returns an iterator which yields a tuple of the module name and 
            the actual module object itself.
        """
        for name in self.list:
            yield (name, self.load(name, objects))
