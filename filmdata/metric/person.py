import logging
from operator import itemgetter
from functools import partial

import filmdata.source
from filmdata.lib.data import Data
from filmdata.metric import Metric

log = logging.getLogger(__name__)

type = 'title'
subtype = 'by ratings'
title = 'top titles by vote ratings'
description = 'calculates the average rating for each title'

interdict = lambda row, cols: [ row[c] for c in cols if c in row ]

class PersonMetric(Metric):

    def __init__(self):
        Metric.__init__(self)
        self._min_votes = {
            'imdb' : 1000,
            'netflix' : 20000,
        }
        self._min_titles = {
            'director' : 4,
            'writer' : 4,
            'cast' : 6,
        }
        self._max_billing = {
            'director' : None,
            'cast' : 8,
            'writer' : 2,
        }
        self._cull_min = self._min_votes[self._cull_source]

    def __call__(self):
        return self.producer()

    def producer(self):
        rows = {}
        bayes_index = {}
        title_ratings = {}

        for (id, group), titles_raw in self._sink.get_person_role_titles():
            titles_filtered = filter(partial(self._title_filter,
                                             role=group), titles_raw)
            is_solid = self._is_solid_role(group, titles_filtered)
            source_index = self._get_source_titles_index(titles_filtered,
                                                         group)
            for source_name, source_ratings in source_index.iteritems():
                means = [ r[self._mean_field] for r in source_ratings ]
                counts = [ r[self._count_field] for r in source_ratings if
                           r.get(self._count_field) ]
                metrics = self._get_source_metrics(means, counts)

                if is_solid:
                    if not group in title_ratings:
                        title_ratings[group] = {}
                        bayes_index[group] = {}
                    if not source_name in title_ratings[group]:
                        title_ratings[group][source_name] = []
                        bayes_index[group][source_name] = []
                    title_ratings[group][source_name].extend(means)
                    bayes_index[group][source_name].append({
                        'id' : id,
                        'mean' : metrics['mean'],
                        'count' : metrics['count'],
                    })

                if not id in rows:
                    rows[id] = {}
                if not group in rows[id]:
                    rows[id][group] = {}
                rows[id][group][source_name] = metrics

        for group, sources in bayes_index.iteritems():
            for source_name, source_data in sources.iteritems():
                data = Data(source_data)
                report_mean = self.numpy.mean(title_ratings[group][source_name])
                data.add_bayes('mean', 'count',
                               self._min_titles[group], report_mean)
                for datum in data:
                    rows[datum['id']][group][source_name]['bayes'] = datum['bayes']

        return rows.iteritems()

    def _get_source_titles_index(self, titles, group):
        source_index = {}
        for title in titles:
            for source_name, source_rating in title['rating'].items():
                if not source_name in source_index:
                    source_index[source_name] = []
                source_index[source_name].append(source_rating)
        return source_index

    def _get_source_metrics(self, means, counts):
        slope = 0
        std = 0
        if len(means) > 2:
            slope = self.stats.linregress(range(0, len(means)), means)[0]
        elif len(means) == 2:
            slope = means[1] - means[0]
        if len(means) > 1:
            std = self.numpy.std(means)

        return {
            'sum' : sum(counts) if counts else None,
            'count' : len(means),
            'mean' : self.numpy.mean(means),
            'median' : self.numpy.median(means),
            'slope' : slope,
            'std' : std,
        }

    def _title_filter(self, title, role=None):
        if not title.get('rating'):
            return False
        if not title['rating'].get(self._cull_source):
            return False
        rating = title['rating'].get(self._cull_source)
        if rating[self._count_field] < self._cull_min:
            return False
        if (self._max_billing.get(role) and
            self._max_billing[role] < title.get('billing', 999)):
            return False
        return True

    def _is_solid_role(self, role, titles):
        if len(titles) < self._min_titles[role]:
            return False
        if role == 'cast':
            titles_top_bill = [ t for t
                                in titles if t['billing'] == 1 ]
            if len(titles_top_bill) < 2:
                return False
        return True
