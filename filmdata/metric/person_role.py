import operator, numpy, logging
from scipy import stats

import filmdata.source
from filmdata import config
from filmdata.lib.data import Data, avg, div, mult

log = logging.getLogger(__name__)

type = 'title'
subtype = 'by ratings'
title = 'top titles by vote ratings'
description = 'calculates the average rating for each title'
_cull_key = '_'.join((config.get('core', 'master_data'), 'votes'))
schema = {
    'person_id' : 'id',
    'role_type' : None,
    'titles_count' : 'integer',
    '_'.join((_cull_key, 'sum')) : 'integer',
}

_data_keys = { 'rating' : {} }

# find all the ratings in the sources and add them to our list
for source_name, source in filmdata.source.manager.iter():
    if 'rating' in source.schema:
        _data_keys['rating'][source_name] = '_'.join((source_name, 'rating'))

# check for multiple sources with ratings
# if true add a new 'average' pseudo source
for field, sources in _data_keys.iteritems():
    if len(sources) > 1:
        _data_keys['rating']['average'] = 'average_rating'

# generate the schema from this list
for data_type, data_sources in _data_keys.iteritems():
    for source_name, data_key in data_sources.iteritems():
        for metric_type in ('mean', 'median', 'std', 'slope', 'bayes'):
            schema_key = '_'.join((source_name, 'rating', metric_type))
            schema[schema_key] = 'decimal'

interdict = lambda row, cols: [ row[c] for c in cols if c in row ]

def titles_filter(titles, role, min=None):
    if min and len(titles) < min:
        return None
    return [ t for t in titles
             if ( t[_cull_key] > 4000 and
                 ( role == 'director' or
                  ( t['billing'] and t['billing'] <= 8 ) ) ) ]

def is_solid_role(role, titles):
    if role != 'director':
        if len(titles) < 6:
            return False
        titles_top_bill = [ t for t
                            in titles if t['billing'] == 1 ]
        if len(titles_top_bill) < 2:
            return False
    elif len(titles) < 4:
        return False
    return True

def persons_filter(persons):
    for person_key, titles_all in persons.iteritems():
        role = person_key[1]
        titles_filtered = titles_filter(titles_all, role, 4)
        if titles_filtered and is_solid_role(person_key[1], titles_filtered):
            yield person_key, titles_filtered

def get_title_data(titles):
    for data_type, data_sources in _data_keys.iteritems():
        for source_name, data_key in data_sources.iteritems():
            if source_name == 'average':
                yield data_key, [
                    float(avg(interdict(t,
                                        _data_keys[data_type].values())))
                    for t in titles ]
            else:
                yield data_key, [ float(t[data_key]) for t in titles ]

def get_title_metrics(data):
    name, values = data
    return {
        '%s_mean' % name : numpy.mean(values),
        '%s_median' % name : numpy.median(values),
        '%s_std' % name : numpy.std(values),
        '%s_slope' % name : stats.linregress(range(0, len(values)), values)[0],
    }

def run(sink):
    data_rows = []
    for k, titles in persons_filter(sink.get_persons_role_titles()):
        titles.sort(key=operator.itemgetter('year'))

        row = {
            'person_id' : k[0],
            'role_type' : k[1],
            'titles_count' : len(titles),
            '_'.join((_cull_key, 'sum')) : sum([ t[_cull_key] for t in
                                                titles ]),
        }

        row.update(reduce(lambda d1, d2: dict(d1.items() + d2.items()),
                          map(get_title_metrics, get_title_data(titles))))
        data_rows.append(row)

    data = Data(data_rows)
    for data_type, data_sources in _data_keys.iteritems():
        for source_name, data_key in data_sources.iteritems():
            data_mean_name = '_'.join((data_key, 'mean'))
            report_mean = data.get_mean(data_mean_name)
            data.add_bayes(data_mean_name, 'titles_count', 4,
                           report_mean, '_'.join((data_key, 'bayes')))
    sink.consume_metric(data.get_rows(include=schema.keys()),
                        'person_role')
