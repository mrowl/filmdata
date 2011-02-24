from filmdata.metric import Data, avg, mult

class Metric:
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

    def __init__(self):
        pass

    def build(self, sink):
        data = Data(sink.get_titles_rating())
        data.add_field('average_rating', 
                       (avg, 'imdb_rating', 'netflix_rating'))
        data.add_field('imdb_rating_sum',
                       (mult, 'imdb_rating', 'imdb_votes'))
        report_mean = data.get_mean('imdb_rating_sum', 'imdb_votes')
        data.add_bayes('imdb_rating', 'imdb_votes', 4000,
                       report_mean, 'imdb_bayes')
        return data.get_rows(include=self.keys)
