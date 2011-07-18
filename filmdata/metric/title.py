import logging

from filmdata.lib.data import Data
from filmdata.metric import Metric

log = logging.getLogger(__name__)

type = 'title'
subtype = 'by ratings'
title = 'top titles by vote ratings'
description = 'calculates the average rating for each title'

schema = {
    'title_id' : 'id',
}

class TitleMetric(Metric):

    def __init__(self):
        Metric.__init__(self)
        self._titles = None

    def __call__(self):
        return self.producer()

    def producer(self):
        self._build_titles()
        indexes = {}
        for source_name in self._sources:
            indexes[source_name] = self._get_source_index(source_name)
        indexes['average'] = self._get_average_index(indexes)
        return self._indexes_to_rows(indexes).iteritems()

    def _build_titles(self):
        if self._titles is None:
            title_list = self._sink.get_title_ratings()
            self._titles = dict([ (t['id'], t['rating']) for t in
                                  title_list if t.get('rating') ])

    def _get_source_index(self, name):
        data = self._get_source_data(name)
        if name in self._count_sources:
            data.add_field('rating_sum',
                           (Data.mult, self._mean_field, self._count_field),
                           arg_min=2)
            report_mean = data.get_mean('rating_sum', self._count_field,
                                        (self._count_field,
                                         self._min_votes[name]))
            data.add_bayes(self._mean_field, self._count_field,
                           self._min_votes[name], report_mean)
        return dict([ (d['id'], d) for d in data ])

    def _get_average_index(self, indexes):
        averages = {}
        for id in self._titles.keys():
            means = [ indexes[n][id]['mean'] for n in
                      self._sources if id in indexes[n] and
                      indexes[n][id].get('mean') is not None ]
            counts = [ indexes[n][id]['count'] for n in
                       self._count_sources if id in indexes[n] and
                       indexes[n][id].get('count') is not None ]
            bayes = [ indexes[n][id]['bayes'] for n in
                      self._count_sources if id in indexes[n] and
                      indexes[n][id].get('bayes') is not None ]
            averages[id] = {
                'mean' : sum(means) / len(means) if counts else None,
                'count' : sum(counts) / len(counts) if counts else None,
                'bayes' : sum(bayes) / len(bayes) if bayes else None,
            }
        return averages

    def _indexes_to_rows(self, indexes):
        rows = {}
        for k, index in indexes.iteritems():
            for id, metrics in index.items():
                if k == 'average':
                    row = metrics
                else:
                    row = {}
                    if metrics.get('bayes'):
                        row['bayes'] = metrics['bayes']
                if row:
                    if not id in rows:
                        rows[id] = {}
                    rows[id][k] = row
        return rows

    def _get_source_data(self, source):
        data = []
        for id, t in self._titles.items():
            if t.get(source) and t[source].get(self._mean_field):
                row = t[source].copy()
                row['id'] = id
                data.append(row)
        return Data(data)
