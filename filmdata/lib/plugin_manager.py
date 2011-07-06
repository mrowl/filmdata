"""
A very simple plugin manager which basically just manages all the modules in
a package.  Plugin == Module throughout.
"""

import os
import pkgutil
from inspect import isclass, getmembers

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

    def __init__(self, pkg_name, pkg_path, objects=('*'), parent_class=None):
        """
        Create a new plugin manager.
        Arguments:
            pkg_name - the name of the package for which to collect modules.
                e.g. filmdata.source
            pkg_path - the path for the package (usually just pass __file__)
            objects - the names of the particular classes, functions, etc. to
                load for each module in the package when using __import__.
                e.g. ('Fetch', 'Produce', 'schema') for a source
            parent_class - instead of loading specific objects, load anything
                that subclasses this parent class
        """
        plugins_path = [os.path.dirname(pkg_path)]
        self.list = [name for _, name, _ in pkgutil.iter_modules(plugins_path)]
        self._pkg_name = pkg_name
        self._objects = objects
        self._parent_class = parent_class

    def load(self, name, objects=None):
        """
        Load a specified module.
        Arguments:
            name - the name of a module, e.g. 'netflix'
            objects - see description of similar arg for __init__
        Returns the module object
        """
        if not name in self.list:
            raise PluginNotFoundException("%s plugin %s not found" %
                                          (self._pkg_name, name))
        if self._parent_class is None:
            if objects == None:
                objects = self._objects
            plugin = __import__('%s.%s' % (self._pkg_name, name),
                                None, None, objects)
        else:
            plugin_mod = __import__('%s.%s' % (self._pkg_name, name),
                                    None, None, '*')
            for obj_name, obj in getmembers(plugin_mod, isclass):
                if (issubclass(obj, self._parent_class) and
                    obj_name != self._parent_class.__name__):
                    plugin = obj
                    break
            else:
                raise PluginNotFoundException("%s plugin %s has no proper subclass" %
                                              (self._pkg_name, name))
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
