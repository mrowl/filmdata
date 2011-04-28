"""
A simple decorator for memoizing functions and methods.
"""

from functools import partial as partial

class memoize(object):
    """
    Decorator class for memoizing functions and methods.
    Attributes:
        func - the function/method that is being decorated
        cache - the dictionary which stores the arguments passed to the func as
            keys and the corresponding value that the func returns as the value
    Usage:
        @memoize
        def multiply(x, y):
            return x * y
    """
    
    def __init__(self, func):
        """
        Create a new memoize object.
        Arguments:
            func - the function to memoize
        """
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        """
        Receives the call to the decorated function/method.
        Arguments:
            args - all the arguments passed to the function
        """
        return self.cache_get(args, lambda: self.func(*args))

    def __get__(self, obj, objtype):
        """
        Handles the special case for methods in classes.
        Arguments:
            obj - the class that contains the decorated method
            objtype - n/a
        """
        return self.cache_get(id(obj),
                              lambda: self.__class__(partial(self.func,
                                                             obj)))

    def cache_get(self, key, gen_callback):
        """
        Searches the cache to see if the result has already been calculated
        for the given key (the args).  Runs the callback func if the results
        weren't cached and adds the result to the dictionary.
        Arguments:
            key - key to search for in the cache dictionary
            gen_callback - the callback to run to generate new results
                and populate the cache
        Returns the cached (hopefully) result of the gen_callback function
        """
        if key not in self.cache:
            self.cache[key] = gen_callback()
        return self.cache[key]
