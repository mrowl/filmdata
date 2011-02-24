from filmdata.metric import Data, avg, div, mult

class Metric:
    type = 'title'
    subtype = 'by ratings'
    title = 'top titles by vote ratings'
    description = 'calculates the average rating for each title'
    keys = (
        'person_id',
        'role_type',
        'titles_count',
        'imdb_votes_sum',
        'imdb_rating_avg',
        'netflix_rating_avg',
        'average_rating_avg',
        'imdb_rating_bayes',
        'netflix_rating_bayes',
        'average_rating_bayes',
    )

    def __init__(self):
        pass

    def build(self, sink):
        data = Data(sink.get_persons_role_titles_agg())
        data.add_field('imdb_rating_avg',
                       (div, 'imdb_rating_sum', 'titles_count'))
        data.add_field('netflix_rating_avg',
                       (div, 'netflix_rating_sum', 'titles_count'))
        data.add_field('average_rating_avg',
                       (avg, 'netflix_rating_avg', 'imdb_rating_avg'))
        data.add_field('average_rating_sum',
                       (mult, 'average_rating_avg', 'titles_count'))
        report_mean = data.get_mean('imdb_rating_sum', 'titles_count')
        data.add_bayes('imdb_rating_avg', 'titles_count', 4,
                       report_mean, 'imdb_rating_bayes')
        report_mean = data.get_mean('netflix_rating_sum', 'titles_count')
        data.add_bayes('netflix_rating_avg', 'titles_count', 4,
                       report_mean, 'netflix_rating_bayes')
        report_mean = data.get_mean('average_rating_sum', 'titles_count')
        data.add_bayes('average_rating_avg', 'titles_count', 4,
                       report_mean, 'average_rating_bayes')
        return data.get_rows(include=self.keys)
