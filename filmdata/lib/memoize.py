import functools

class memoize(object):
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        return self.cache_get(args, lambda: self.func(*args))

    def __get__(self, obj, objtype):
        return self.cache_get(id(obj), lambda: self.__class__(functools.partial(self.func, obj)))

    def cache_get(self, key, gen_callback):
        if key not in self.cache:
            self.cache[key] = gen_callback()
        return self.cache[key]
