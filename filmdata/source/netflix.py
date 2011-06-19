import logging, re, HTMLParser, os, itertools, string
from decimal import Decimal
import oauth2 as oauth

import xml.etree.cElementTree as etree

from filmdata import config

log = logging.getLogger(__name__)

schema = {
    'title_id' : 'id',
    'rating' : None,
    'key' : 'integer',
    'art_small' : 'varchar(100)',
    'art_large' : 'varchar(100)',
    'runtime' : 'integer',
    'bluray' : 'tinyint',
    'instant_quality' : 'tinyint',
    'instant_expires' : 'timestamp',
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
    _re_film_test = re.compile('http://api.netflix.com/catalog/titles/movies/([0-9]+)')
    _re_art_small = re.compile('<link href="([^"]*?)" rel="http://schemas.netflix.com/catalog/titles/box_art/150pix_w"')
    _re_art_large = re.compile('<link href="([^"]*?)" rel="http://schemas.netflix.com/catalog/titles/box_art/284pix_w"')
    _re_instant = re.compile('<category label="instant" scheme="http://api.netflix.com/categories/title_formats" term="instant">')
    _re_instant_hd = re.compile('<category label="HD" scheme="http://api.netflix.com/categories/title_formats/quality" term="HD"/>')
    _re_bluray = re.compile('<category label="Blu-ray" scheme="http://api.netflix.com/categories/title_formats" term="Blu-ray">')

    _fieldsets = { 
        'key' : { 'name' : _re_name, 'year' : _re_year },
        'data' : { 'key' : _re_film_test,
                   'art_small' : _re_art_small,
                   'art_large' : _re_art_large,
                   'instant' : _re_instant,
                   'instant_hd' : _re_instant_hd,
                   'bluray' : _re_bluray,
                   'rating' : _re_rating }
    }

    @classmethod
    def produce_data(cls, types):
        for title in cls._get_titles(types):
            yield title

    @classmethod
    def _get_titles(cls, types=None):
        h = HTMLParser.HTMLParser()
        clean = lambda x: unicode(h.unescape(x))

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
                
        def _form_title_dict(elem, netflix_key):
            release_year = elem.find('release_year')
            if release_year == None or release_year.text == None:
                log.info('Year not found on %d' % netflix_key)
                return None

            title_key = {
                'name' : clean(elem.find('title').get('regular')), 
                'year' : int(release_year.text),
            }

            netflix_values = {
                'key' : netflix_key,
                'rating' : Decimal(elem.find('average_rating').text) * cls._rating_factor,
                'runtime' : 0,
                'bluray' : 0,
                'instant_quality' : None,
                'instant_expires' : None,
            }

            box_art = elem.find('./link[@rel="http://schemas.netflix.com/catalog/titles/box_art"]/box_art')
            if box_art != None:
                art_nodes = {
                    'small' : box_art.find('./link[@rel="http://schemas.netflix.com/catalog/titles/box_art/150pix_w"]'),
                    'large' : box_art.find('./link[@rel="http://schemas.netflix.com/catalog/titles/box_art/284pix_w"]'),
                }
                for name, node in art_nodes.items():
                    key = '_'.join(('art', name))
                    if node != None:
                        netflix_values[key] = node.get('href')
                    else:
                        netflix_values[key] = None
            else:
                log.info('No box art found for %s' % title_key['name'])

            availabilities = list(elem.find('./link[@rel="http://schemas.'
                                            'netflix.com/catalog/titles/f'
                                            'ormat_availability"]/delivery'
                                            '_formats'))
            for availability in availabilities:
                format = availability.find('./category[@scheme="http://api.netflix.com/categories/title_formats"]')
                if not format:
                    log.warn('No format info found for %s' % title_key['name'])
                    continue
                if format.get('label') == 'Blu-ray':
                    netflix_values['bluray'] = 1
                elif format.get('label') == 'instant':
                    quality = format.find('./category[@scheme="http://api.netflix.com/categories/title_formats/quality"]')
                    if quality != None and quality.get('label') == 'HD':
                        netflix_values['instant_quality'] = 2
                    else:
                        netflix_values['instant_quality'] = 1
                    netflix_values['instant_expires'] = int(
                        availability.get('available_until'))
                runtime_node = availability.find('runtime')
                if runtime_node != None:
                    netflix_values['runtime'] = int(runtime_node.text)

            return [ title_key, [ 'netflix', netflix_values ] ]

        context = etree.iterparse(cls._titles_file_path,
                                  events=('start', 'end'))
        context = iter(context)
        event, root = context.next()
        for event, elem in context:
            if event == 'end' and elem.tag == 'catalog_title':
                film_match = cls._re_film_test.match(elem.find('id').text)
                if film_match:
                    title = _form_title_dict(elem, int(film_match.group(1)))
                    if title != None:
                        yield title
                elem.clear()
                root.clear()

if __name__ == '__main__':
    Fetch.fetch_data()
