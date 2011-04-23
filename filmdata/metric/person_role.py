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
_metric_keys = { 'average' : {} }
for source_name, source in filmdata.source.manager.iter():
    if 'rating' in source.schema:
        for metric_type in ('mean', 'median', 'std', 'slope', 'bayes'):
            schema_key = '_'.join((source_name, 'rating', metric_type))
            schema[schema_key] = 'decimal'
        _data_keys['rating'][source_name] = '_'.join((source_name, 'rating'))

for field, sources in _data_keys.iteritems():
    if len(sources) > 1:
        for metric_type in ('mean', 'median', 'std', 'slope', 'bayes'):
            schema_key = '_'.join(('average', field, metric_type))
            schema[schema_key] = 'decimal'
        _metric_keys['average'][field] = '_'.join(('average', field))

def run(sink):
    data_rows = []
    for k, titles_all in sink.get_persons_role_titles().iteritems():
        if len(titles_all) < 4:
            continue
        titles_filtered = [ t for t in titles_all
                            if t[_cull_key] > 4000
                            and (k[1] == 'director'
                                 or (t['billing'] and t['billing'] <= 8)) ]
        if k[1] != 'director':
            if len(titles_filtered) < 6:
                continue
            titles_top_bill = [ t for t in titles_filtered if t['billing'] == 1 ]
            if len(titles_top_bill) < 2:
                continue
        else:
            if len(titles_filtered) < 4:
                continue

        titles_filtered.sort(key=operator.itemgetter('year'))
        title_arrays = {}
        for data_type, data_sources in _data_keys.iteritems():
            for source_name, data_key in data_sources.iteritems():
                title_arrays[data_key] = [ float(t[data_key]) for t
                                           in titles_filtered ]
        interdict = lambda row, cols: [ row[c] for c in cols if c in row ]
        for metric_type, metric_sources in _metric_keys.iteritems():
            for source_name, metric_key in metric_sources.iteritems():
                if metric_type == 'average':
                    title_arrays[metric_key] = [ float(avg(interdict(t, _data_keys[source_name].values()))) for t in titles_filtered ]

        titles_count = len(titles_filtered)

        row = {
            'person_id' : k[0],
            'role_type' : k[1],
            'titles_count' : len(titles_filtered),
            '_'.join((_cull_key, 'sum')) : sum([ t[_cull_key] for t in
                                                titles_filtered ]),
        }

        for name, values in title_arrays.iteritems():
            row['%s_mean' % name] = numpy.mean(values)
            row['%s_median' % name] = numpy.median(values)
            row['%s_std' % name] = numpy.std(values)
            row['%s_slope' % name] = stats.linregress(range(0, titles_count),
                                                      values)[0]

        data_rows.append(row)

    log.info('%d rows for person_role' % len(data_rows))
    data = Data(data_rows)
    for data_type, data_sources in _data_keys.iteritems():
        for source_name, data_key in data_sources.iteritems():
            data_mean_name = '_'.join((data_key, 'mean'))
            report_mean = data.get_mean(data_mean_name)
            data.add_bayes(data_mean_name, 'titles_count', 4,
                           report_mean, '_'.join((data_key, 'bayes')))
    sink.consume_metric(data.get_rows(include=schema.keys()),
                        'person_role')
