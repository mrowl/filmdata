import logging, re, HTMLParser, os, itertools, string
from decimal import Decimal
import oauth2 as oauth

from filmdata import config

log = logging.getLogger(__name__)

schema = {
    'title_id' : 'id',
    'rating' : None,
    'key' : 'integer',
}

class NetflixClient(oauth.Client):

    def __init__(self, *args, **kwargs):
        self._initial_redirections = None
        oauth.Client.__init__(self, *args, **kwargs)
        
    def request(self, *args, **kwargs):
        if not 'redirections' in kwargs:
            kwargs['redirections'] = oauth.httplib2.DEFAULT_MAX_REDIRECTS
        if self._initial_redirections is None:
            self._initial_redirections = kwargs['redirections']

        if kwargs['redirections'] < self._initial_redirections:
            return oauth.httplib2.Http.request(self, *args, **kwargs)
        else:
            return oauth.Client.request(self, *args, **kwargs)

class NetflixMixin:
    """
    Mixin which holds some common netflix functions/variables.
    Attributes:
        name - the name of this module
        _rating_factor - the factor to multiply the ratings by (the global
            maximum rating / the local max rating). Netflix max rating is 5.
        _titles_file_path - the path to the xml file containing the netflix
            titles index
        _titles_dir_path - the path to the directory holding all of the
            xml files for each individual netflix title
    """

    name = 'netflix'
    _rating_factor = int(config.core.max_rating) / 5

    _titles_file_path = config.netflix.titles_xml_path
    _titles_dir_path = config.netflix.titles_dir_path

    @classmethod
    def _get_title_path(cls, id):
        """
        Get the os path to a title based on its id.
        Arguments:
            id - the netflix id of the title.
        Returns a full path.
        """
        basename = '.'.join((id, 'xml'))
        bucket = id[:2]
        return os.path.join(cls._titles_dir_path, bucket, basename)


class Fetch(NetflixMixin):

    _consumer_key = config.netflix.consumer_key
    _consumer_secret = config.netflix.consumer_secret
    _titles_url = config.netflix.titles_url
    _title_url_base = config.netflix.title_url_base
    _call_limit = 4900

    @classmethod
    def fetch_data(cls):
        cls._download_title_catalog()

    @classmethod
    def _download_title_catalog(cls):
        if not os.path.isdir(os.path.dirname(cls._titles_file_path)):
            os.makedirs(os.path.dirname(cls._titles_file_path))
        xml = cls._fetch(cls._titles_url)
        f = open(cls._titles_file_path, 'w')
        f.write(xml)
        f.close()

    @classmethod
    def _fetch(cls, url):
        consumer = oauth.Consumer(key=cls._consumer_key, secret=cls._consumer_secret)
        client = NetflixClient(consumer)

        print url
        client.follow_all_redirects = True
        resp, content = client.request(url, "GET", headers={'accept-encoding' : 'gzip'})
        if resp['status'] == '403':
            raise Exception('Over Netflix API rate limit?: %s\n%s' %
                            (str(resp), content))
        elif resp['status'] != '200' or content.split() == '':
            raise Exception('Unknown issue with netflix API: %s\n%s' %
                            (str(resp), content))
        return content

class Produce(NetflixMixin):

    _re_name = re.compile('<title regular="(.*?)" short=".*?"/>')
    _re_year = re.compile('<release_year>([0-9]+)</release_year>')
    _re_rating = re.compile('<average_rating>([0-9]\.[0-9])</average_rating>')
    _re_tv_test = re.compile('<category.*? label="Television" term="Television">')
    _re_film_test = re.compile('<id>http://api.netflix.com/catalog/titles/movies/([0-9]+)</id>')

    _fieldsets = { 
        'key' : { 'name' : _re_name, 'year' : _re_year },
        'data' : { 'key' : _re_film_test, 'rating' : _re_rating }
    }

    @classmethod
    def produce_data(cls, types):
        for title in cls._get_titles(types):
            yield title

    @classmethod
    def _get_titles(cls, types=None):
        def _extract_catalog_titles(lines):
            buffer = ''
            for line in lines:
                if line ==  '</catalog_title>':
                    yield buffer + line
                    buffer = ''
                elif line ==  '<catalog_title>':
                    buffer = line
                else:
                    buffer += line
                
        def _form_title_dict(title_xml):
            values = { 'key' : { 'type' : 'film' }, 'data' : {} }
            for field_type, fieldset in cls._fieldsets.items():
                for field_key, field_re in fieldset.items():
                    match = field_re.search(title_xml)
                    if match is not None:
                        values[field_type][field_key] = match.group(1).strip()
                    else:
                        log.info('Title missing field: %s' % field_key)
                        return None
            values['data']['key'] = int(values['data']['key'])
            values['data']['rating'] = (Decimal(values['data']['rating']) *
                                        cls._rating_factor)
            return [ values['key'], [ 'netflix', values['data'] ] ]

        h = HTMLParser.HTMLParser()
        clean_xml = lambda x: unicode(h.unescape(x.decode('utf-8')))
        is_type_film = lambda t: (cls._re_film_test.search(t) and not
                                  cls._re_tv_test.search(t))

        f = open(cls._titles_file_path, 'r')
        return itertools.ifilter(None,
            itertools.imap(_form_title_dict,
                itertools.ifilter(is_type_film,
                    itertools.imap(clean_xml,
                        _extract_catalog_titles(
                            itertools.imap(string.strip, f))))))

if __name__ == '__main__':
    Fetch.fetch_data()
