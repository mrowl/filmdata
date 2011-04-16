import os, pkgutil

from filmdata import config

sources_path = [os.path.dirname(__file__)]
list = [name for _, name, _ in pkgutil.iter_modules(sources_path)]
source_objects = ('Fetch', 'Produce', 'schema')

def load(name, objects=None):
    if objects == None:
        objects = source_objects
    source = __import__('filmdata.sources.%s' % name,
                        None, None, objects)
    return source

def iter(objects=None):
    for name in list:
        yield (name, load(name, objects))
