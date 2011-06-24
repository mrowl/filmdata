import logging, re, HTMLParser, os, string
from datetime import datetime
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
    'synopsis_long' : 'varchar(1023)',
    'synopsis_short' : 'varchar(511)',
    'dvd_from' : 'datetime',
    'bluray_from' : 'datetime',
    'instant_from' : 'datetime',
    'instant_until' : 'datetime',
    'instant_quality' : 'tinyint',
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
    def produce_titles(cls, types):
        for title in cls._get_titles(types):
            title['master'] = True
            yield title

    @classmethod
    def _get_titles(cls, types=None):
        h = HTMLParser.HTMLParser()
        clean = lambda x: unicode(h.unescape(x))

        def _get_availability(elem):
            format = elem.find('./category[@scheme='
                               '"http://api.netflix.com/categories/title_formats"]')
            if not format:
                log.warn('No format info found')
                return None

            avail = {
                'from' : elem.get('available_from'),
                'until' : elem.get('available_until'),
            }
            for k in avail.keys():
                if avail[k] != None:
                    avail[k] = datetime.fromtimestamp(int(avail[k]))
            quality = format.find('./category[@scheme='
                                  '"http://api.netflix.com/categories/title_formats/quality"]')
            if quality != None and quality.get('label') == 'HD':
                avail['quality'] = 2
            else:
                avail['quality'] = 1
            runtime_node = elem.find('runtime')
            if runtime_node != None:
                avail['runtime'] = int(runtime_node.text)

            label_to_key_map = {
                'Blu-ray' : 'bluray',
                'instant' : 'instant',
                'DVD' : 'dvd',
            }
            label = format.get('label')
            if not label in label_to_key_map:
                log.warn('Unknown format %s' % label)
                return None

            return { label_to_key_map[label] : avail }

        def _get_availabilities(elem):
            elems = list(elem.find('./link[@rel="http://schemas.'
                           'netflix.com/catalog/titles/f'
                           'ormat_availability"]/delivery'
                           '_formats'))
            avails = {}
            for avail in [ a for a in map(_get_availability, elems) if a ]:
                avails.update(avail)
            return avails if len(avails) > 0 else None
                
        def _get_art(elem):
            box_art = elem.find('./link[@rel="http://schemas.netflix.com'
                                '/catalog/titles/box_art"]/box_art')
            if box_art == None:
                log.info('No box art found for')
                return None

            art = {
                'small' : box_art.find('./link[@rel="http://schemas.netflix.com'
                                       '/catalog/titles/box_art/150pix_w"]'),
                'large' : box_art.find('./link[@rel="http://schemas.netflix.com'
                                       '/catalog/titles/box_art/284pix_w"]'),
            }
            for key, node in art.items():
                if node != None:
                    art[key] = node.get('href')
            return art

        def _get_synopsis(elem):
            synopsis = {}
            long = elem.find('./link[@rel="http://schemas.netflix.com'
                             '/catalog/titles/synopsis"]/synopsis')
            if long != None:
                synopsis['long'] = clean(long.text)

            short = elem.find('./link[@rel="http://schemas.netflix.com'
                              '/catalog/titles/synopsis.short"]/short_synopsis')
            if short != None:
                synopsis['short'] = clean(short.text)
            return synopsis if len(synopsis) > 0 else None

        def _get_genres(elem):
            categories = elem.findall('./category')
            genres = []
            for cat in categories:
                scheme = cat.get('scheme')
                if 'genres' in scheme:
                    genres.append(clean(cat.get('label')))
            return genres if len(genres) > 0 else None

        def _form_title_dict(elem, netflix_key):
            release_year = elem.find('release_year')
            if release_year == None or release_year.text == None:
                log.info('Year not found on %d' % netflix_key)
                return None

            title = {
                'key' : netflix_key,
                'name' : clean(elem.find('title').get('regular')), 
                'year' : int(release_year.text),
                'rating' : Decimal(elem.find('average_rating').text) * cls._rating_factor,
                'synopsis' : _get_synopsis(elem),
                'availability' : _get_availabilities(elem),
                'art' : _get_art(elem),
                'genre' : _get_genres(elem),
            }

            return title

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
