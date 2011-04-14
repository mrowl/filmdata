from yapsy.IPlugin import IPlugin

from filmdata.metric import Data, avg, mult
from filmdata import sink

class MetricTitle(IPlugin):
    type = 'title'
    subtype = 'by ratings'
    title = 'top titles by vote ratings'
    description = 'calculates the average rating for each title'
    keys = (
        'title_id',
        'imdb_rating',
        'imdb_votes',
        'imdb_bayes',
        'netflix_rating',
        'average_rating',
    )

    def activate(self):
        data = Data(sink.get_titles_rating())
        data.add_field('average_rating', 
                       (avg, 'imdb_rating', 'netflix_rating'))
        data.add_field('imdb_rating_sum',
                       (mult, 'imdb_rating', 'imdb_votes'))
        report_mean = data.get_mean('imdb_rating_sum', 'imdb_votes')
        data.add_bayes('imdb_rating', 'imdb_votes', 4000,
                       report_mean, 'imdb_bayes')
        sink.consume_metric(data.get_rows(include=self.keys), 'metric_title')
