import logging

from filmdata.lib.data import Data, avg, mult

log = logging.getLogger(__name__)

type = 'title'
subtype = 'by ratings'
title = 'top titles by vote ratings'
description = 'calculates the average rating for each title'
schema = {
    'title' : 'id',
    'imdb_rating' : 'decimal',
    'imdb_votes' : 'integer',
    'imdb_bayes' : 'decimal',
    'netflix_rating' : 'decimal',
    'average_rating' : 'decimal',
}

def run(sink):
    data = Data(sink.get_titles_rating())
    data.add_field('average_rating', 
                   (avg, 'imdb_rating', 'netflix_rating'))
    data.add_field('imdb_rating_sum',
                   (mult, 'imdb_rating', 'imdb_votes'))
    report_mean = data.get_mean('imdb_rating_sum', 'imdb_votes')
    data.add_bayes('imdb_rating', 'imdb_votes', 4000,
                   report_mean, 'imdb_bayes')
    sink.consume_metric(data.get_rows(include=schema.keys()),
                        'title')
