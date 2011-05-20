"""
For accessing dictionaries with dot notation.
"""

class dotdict(dict):
    """
    This class operates like the standard dictionary except you can use
    dot notation for accessing values.
    Example:
        my_dict = dotdict()
        my_dict['thing'] = 'what'
        my_dict.thing  # 'what'
        my_dict['thing']  # 'what'
    """

    def __getattr__(self, attr):
        """Wraps around the normal dictionary getter"""
        return self.get(attr, None)

    #__setattr__ = dict.__setitem__ don't allow setting with dot notation
    __delattr__ = dict.__delitem__
