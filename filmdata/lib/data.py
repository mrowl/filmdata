"""
Provides a class for a data table.
"""

from filmdata.lib.memoize import memoize

class Data:
    """
    Class models data like a db table.  It's basically a list (rows) of
    dictionaries (columns).

    Attributes:
        bayes - static lambda function for calculating a true bayesian est.
            R = average for the movie (mean) = (Rating)
            v = number of votes for the movie = (votes)
            m = minimum votes required to be in the list
            C = the mean vote across the whole report
        mult - multiply
        div - divide
        avg - mean value of a list
        rows - the table rows (a python list, each row is a dictionary)
        index - the current index for iteration

    Example:
        rows = [
            {'city' : 'New York', 'temp' : '89', 'pop' : '8000000'},
            {'city' : 'Atlanta', 'temp' : '100', 'pop' : '1000000'},
        ]
        data = Data(rows)
        data.sort('temp') # sorts by temperature (asc)
        # add a new column/field to the table which is the product of the
        # temp and pop of each row
        data.add_field('multiplied', (Data.mult, 'temp', 'pop'))
    """
    
    bayes = staticmethod(lambda R, v, m, C: ((R * v) + (C * m)) / (v + m))
    mult = staticmethod(lambda x, y: x * y)
    div = staticmethod(lambda x, y: x / y)
    avg = staticmethod(lambda x: sum(x) / len(x))

    def __init__(self, rows):
        """
        Create a new data table.
        Arguments:
            rows - a list of dictionaries which have the same keys
        """
        self.rows = rows
        self.index = 0

    def __iter__(self):
        """ Magic method for iteration. """
        self.index = 0
        return self

    def next(self):
        """ Magic method for iteration. """
        if self.index == len(self.rows):
            raise StopIteration
        else:
            self.index += 1
            return self.rows[self.index - 1]

    def get_rows(self, include=None, exclude=None):
        """
        Get the table rows with certain rows included or excluded.
        Arguments:
            include - a list of columns/keys to include in the returned rows
            exclude - a list of columns/keys to exclude in the returned rows
        Returns table rows.
        """
        assert include is None or exclude is None

        def remove_keys(source, keys):
            """ Remove keys from a dict """
            for k in keys:
                if k in source:
                    del source[k]
            return source

        def keep_keys(source, keys):
            """ Remove keys not in list from a dict """
            for k in source.keys():
                if not k in keys:
                    del source[k]
            return source

        if include:
            return [ keep_keys(r, include) for r in self.rows ]
        elif exclude:
            return [ remove_keys(r, exclude) for r in self.rows ]
        else:
            return self.rows

    def get_index(self, key):
        """
        Sort the rows based on the key and then return an index off of it.
        You can then look up the row # for a city with index['Atlanta']
        Arguments:
            key - the key to index on
        Returns a dictionary where the keys are the values of the given column
            from each row and the values are the row number
        """
        self.sort(key)
        return dict((r[key], r) for r in self.rows)

    def sort(self, key, reverse=False):
        """
        Sort the rows on the given key, in place.
        Arguments:
            key - the key on which to sort
            reverse - boolean specifying whether to sort in
                ascending or descending order
        Returns nothing.
        """
        sorted_keys = [ (r[key], i)  for i, r in enumerate(self.rows) ]
        sorted_keys.sort()
        if reverse:
            sorted_keys.reverse()
        self.rows = [ self.rows[i] for _, i in sorted_keys ]

    def print_rows(self):
        """ Helper for printing all the rows (useful for debugging) """
        for r in self.rows:
            print r

    def add_field(self, name, ops, arg_min=1):
        """
        Add a new column/field to each of the rows/dictionaries in the table.
        See class example for more info.
        Arguments:
            name - the name of the new column
            ops - a sequence for how to generate the value in the new column
                ops[0] is an operator
                ops[1:] are the column names on which ops[0] will operate
                    (i.e. the operands)
        Returns nothing.
        """
        new_rows = []
        for r in self.rows:
            args = [ r[f] for f in ops[1:] if r.get(f) is not None ]
            if len(args) >= arg_min:
                r[name] = ops[0](*args)
            else:
                r[name] = None
            new_rows.append(r)
        self.rows = new_rows

    def add_bayes(self, r_field, v_field, m_val, c_val, label='bayes'):
        """
        Add a bayesian estimate to each row. See class attributes for a
        description of this function.
        Arguments:
            R_field - the name of the column which holds the r argument
            v_field - the name of the column which holds the v argument
            m_val - the minimum number of inputs
            C_val - the report mean (should have come from all the rows)
            label - the name of the new column containing the bayes estimate
        Returns nothing.
        """
        new_rows = []
        for r in self.rows:
            if r.get(r_field) and r.get(v_field) and r[v_field] > m_val:
                r[label] = self.bayes(r[r_field], r[v_field], m_val, c_val)
            else:
                r[label] = None
            new_rows.append(r)
        self.rows = new_rows

    def add_average(self, fields, name='average'):
        """
        Add a new 'average' column to each row, in place.
        e.g. data.add_average(('temp', 'pop'))
        Arguments:
            fields - the names of the existing columns to average across
            name - the name of this new column
        Returns nothing.
        """
        for i in range(0, len(self.rows)):
            self.rows[i][name] = sum([self.rows[i][f] for f in fields])\
                / len(fields)

    def get_sum(self, field, min=None):
        """
        Get the sum of one column across the table.
        Arguments:
            field - the name of the column to sum
        Returns a number representing the sum.
        """
        if not min:
            return sum([ r[field] for r in self.rows ])
        else:
            return sum([ r[field] for r in self.rows if
                         r.get(min[0]) and r[min[0]] > min[1] ])

    def get_count(self, min=None):
        """
        Get the total number of rows.
        Returns integer of the number of rows in the table.
        """
        if not min:
            return len(self.rows)
        else:
            return len([ 1 for r in self.rows if
                         r.get(min[0]) and r[min[0]] > min[1] ])

    @memoize
    def get_mean(self, field, divisor_sum=None, min=None):
        """
        Get the mean for one column across the table.
        Arguments:
            field - the name of the column for which to find the mean
            divisor_sum - optionally divide the chosen column sum by another
                sum, instead of the table count
        Returns a number representing a mean.
        """
        if not divisor_sum:
            return self.get_sum(field, min=min) / self.get_count(min=min)
        else:
            return self.get_sum(field, min=min) / self.get_sum(divisor_sum, min=min)
