import functools

BAYES = lambda R, v, m, C: ((R * v) + (C * m)) / (v + m)
mult = lambda x, y: x * y
div = lambda x, y: x / y
avg = lambda x, y: (x + y) / 2

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

class Data:
    def __init__(self, rows):
        self.rows = rows
        self.index = 0

    def __iter__(self):
        self.index = 0
        return self

    def next(self):
        if self.index == len(self.rows):
            raise StopIteration
        else:
            self.index += 1
            return self.rows[self.index - 1]

    def get_rows(self, include=None, exclude=None):
        def remove_keys(dict, keys):
            for k in keys:
                if k in dict:
                    del dict[k]
            return dict

        def keep_keys(dict, keys):
            for k in dict.keys():
                if not k in keys:
                    del dict[k]
            return dict

        if include:
            return [ keep_keys(r, include) for r in self.rows ]
        elif exclude:
            return [ remove_keys(r, exclude) for r in self.rows ]
        else:
            return self.rows

    def get_index(self, key):
        self.sort(key)
        return dict((t[key], t) for t in self.rows)

    def add_field(self, field_name, field_def):
        new_rows = []
        for r in self.rows:
            args = [ r[f] for f in field_def[1:] if f in r ]
            r[field_name] = field_def[0](*args)
            new_rows.append(r)
        self.rows = new_rows

    def sort(self, key, reverse=False):
        sorted_keys = [ (r[key], i)  for i, r in enumerate(self.rows) ]
        sorted_keys.sort()
        reverse and sorted_keys.reverse()
        self.rows = [ self.rows[i] for v, i in sorted_keys ]

    def print_rows(self):
        for r in self.rows:
            print r

    @memoize
    def get_sum(self, field):
        return sum([ r[field] for r in self.rows ])

    @memoize
    def get_count(self):
        return len(self.rows)

    @memoize
    def get_mean(self, field, divisor_sum=None):
        if not divisor_sum:
            return self.get_sum(field) / self.get_count()
        else:
            return self.get_sum(field) / self.get_sum(divisor_sum)

    @memoize
    def add_bayes(self, R_field, v_field, m_val, C_val, label='bayes'):
        new_rows = []
        for r in self.rows:
            r[label] = BAYES(r[R_field], r[v_field], m_val, C_val)
            new_rows.append(r)
            #print str(r[R_field]) + ' ' + str(r[bayes])
        self.rows = new_rows

    @memoize
    def add_average(self, x, y, name='average'):
        for i in range(0, len(self.rows)):
            self.rows[i][name] = (self.rows[i][x] + self.rows[i][y]) / 2
