import logging
from collections import defaultdict

from filmdata.lib.data import Data, avg, mult
import filmdata.source

log = logging.getLogger(__name__)

type = 'title'
subtype = 'by ratings'
title = 'top titles by vote ratings'
description = 'calculates the average rating for each title'

schema = {
    'title_id' : 'id',
}

_data_types = { 'votes' : 'integer', 'rating' : 'decimal' }
_data_keys = { 'votes' : {}, 'rating' : {}, 'bayes' : {} }
_metric_keys = { 'average' : {} }
for source_name, source in filmdata.source.manager.iter():
    for data_type in _data_types.keys():
        if data_type in source.schema:
            schema_key = '_'.join((source_name, data_type))
            schema[schema_key] = _data_types[data_type]
            _data_keys[data_type][source_name] = schema_key
    if source_name in _data_keys['votes'] and source_name in _data_keys['rating']:
        schema_key = '_'.join((source_name, 'bayes'))
        schema[schema_key] = 'decimal'
        _data_keys['bayes'][source_name] = schema_key

for field, sources in _data_keys.iteritems():
    if len(sources) > 1:
        schema_key = '_'.join(('average', field))
        schema[schema_key] = 'decimal'
        _metric_keys['average'][field] = schema_key

_min_votes = 4000

def run(sink):
    data = Data(sink.get_titles_rating(_min_votes))

    for source_name, data_key in _data_keys['bayes'].iteritems():
        rating_name = _data_keys['rating'][source_name]
        votes_name = _data_keys['votes'][source_name]
        sum_name = '_'.join((rating_name, 'sum'))
        data.add_field(sum_name, (mult, rating_name, votes_name))
        report_mean = data.get_mean(sum_name, votes_name)
        data.add_bayes(rating_name, votes_name, _min_votes,
                       report_mean, data_key)
    for metric_name, sources in _metric_keys.iteritems():
        for data_type, metric_key in sources.iteritems():
            if metric_name == 'average':
                data.add_average(_data_keys[data_type].values(), metric_key)

    sink.consume_metric(data.get_rows(include=schema.keys()), 'title')
