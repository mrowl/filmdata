import operator, numpy, logging
from scipy import stats

from filmdata.lib.data import Data, avg, div, mult

log = logging.getLogger(__name__)

type = 'title'
subtype = 'by ratings'
title = 'top titles by vote ratings'
description = 'calculates the average rating for each title'
schema = {
    'person' : 'id',
    'role_type' : None,
    'titles_count' : 'integer',
    'imdb_votes_sum' : 'integer',
    'imdb_rating_mean' : 'decimal',
    'netflix_rating_mean' : 'decimal',
    'average_rating_mean' : 'decimal',
    'imdb_rating_median' : 'decimal',
    'netflix_rating_median' : 'decimal',
    'average_rating_median' : 'decimal',
    'imdb_rating_std' : 'decimal',
    'netflix_rating_std' : 'decimal',
    'average_rating_std' : 'decimal',
    'imdb_rating_slope' : 'decimal',
    'netflix_rating_slope' : 'decimal',
    'average_rating_slope' : 'decimal',
    'imdb_rating_bayes' : 'decimal',
    'netflix_rating_bayes' : 'decimal',
    'average_rating_bayes' : 'decimal',
}

def run(sink):
    data_rows = []
    for k, titles_all in sink.get_persons_role_titles().iteritems():
        if len(titles_all) < 4:
            continue
        titles_filtered = [ t for t in titles_all
                            if t['imdb_votes'] > 4000
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
        title_arrays = {
            'imdb_rating' : [ float(t['imdb_rating']) for t in titles_filtered ],
            'netflix_rating' : [ float(t['netflix_rating']) for t in titles_filtered ],
            'average_rating' : [ float(avg(t['netflix_rating'], t['imdb_rating'])) for t in titles_filtered ],
        }

        titles_count = len(titles_filtered)

        row = {
            'person_id' : k[0],
            'role_type' : k[1],
            'titles_count' : len(titles_filtered),
            'imdb_votes_sum' : sum([ t['imdb_votes'] for t in titles_filtered ]),
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
    report_mean = data.get_mean('imdb_rating_mean')
    data.add_bayes('imdb_rating_mean', 'titles_count', 4,
                   report_mean, 'imdb_rating_bayes')
    report_mean = data.get_mean('netflix_rating_mean')
    data.add_bayes('netflix_rating_mean', 'titles_count', 4,
                   report_mean, 'netflix_rating_bayes')
    report_mean = data.get_mean('average_rating_mean')
    data.add_bayes('average_rating_mean', 'titles_count', 4,
                   report_mean, 'average_rating_bayes')
    sink.consume_metric(data.get_rows(include=schema.keys()),
                        'person_role')
