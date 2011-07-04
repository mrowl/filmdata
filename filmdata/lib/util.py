import string
import re
import json
ALPHA_36 = ''.join((string.digits, string.ascii_lowercase))
ALPHA_62 = ''.join((ALPHA_36, string.ascii_uppercase))

def base_encode(number, base=36):
    """Convert positive integer to a base36 string."""
    if not isinstance(number, (int, long)):
        raise TypeError('number must be an integer')
    if base == 62:
        alphabet = ALPHA_62
    else:
        alphabet = ALPHA_36
 
    # Special case for zero
    if number == 0:
        return alphabet[0]

    base = ''
 
    sign = ''
    if number < 0:
        sign = '-'
        number = - number
 
    while number != 0:
        number, i = divmod(number, len(alphabet))
        base = alphabet[i] + base
 
    return sign + base

def base_decode(number, base=36):
    return int(number, base)

def rname(n):
    a = n.split(',')
    return ' '.join(a[1:]).partition('(')[0].strip() + ' ' + a[0]

clean_name = lambda x: re.sub('\(.*?\)', '', x)

class dson:

    @staticmethod
    def dump(data, path, append=False):
        assert isinstance(data, dict)
        keys = sorted(data.keys())
        mode = 'a' if append else 'w'
        f = open(path, mode)
        for k in keys:
            f.write(json.dumps((k, data[k])) + "\n")
        f.close()

    @staticmethod
    def load(path):
        data = {}
        f = open(path)
        for item in map(json.loads, f):
            data[item[0]] = item[1]
        f.close()
        return data

class class_property(property):
    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()
