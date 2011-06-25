import logging, re, HTMLParser, os, string
from datetime import datetime
from decimal import Decimal
import oauth2 as oauth
from functools import partial

import xml.etree.cElementTree as etree
from tornado import httpclient, httputil
import tornado.ioloop
from cookielib import MozillaCookieJar
from Cookie import SimpleCookie

import json

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
    _master_lock = False
    _thread_finished_count = 0
    _thread_count = 50

    @classmethod
    def fetch_data(cls):
        cls._download_title_catalog()

    @classmethod
    def fetch_votes(cls):
        re_link = re.compile('^<link href="(http://www.netflix.com/Movie/[^/]+/[0-9]+)" rel="alternate" title="web page"/>$')
        re_votes = re.compile('^\s+Average of ([0-9,]+) ratings:\s*$')
        re_key = re.compile('^http://www.netflix.com/Movie/[^/]+/([0-9]+)')
        headers = httputil.HTTPHeaders()

        jar = MozillaCookieJar(config.netflix.cookies_txt_path)
        jar.load()
        for line in jar:
            cookie = SimpleCookie()
            cookie[line.name] = line.value
            for key in ('domain', 'expires', 'path'):
                cookie[line.name][key] = getattr(line, key)
            partitions = cookie.output().partition(' ') 
            headers.add(partitions[0].replace('Set-', ''), partitions[2])

        urls = []
        for line in open(cls._titles_file_path):
            link_match = re_link.match(line.strip())
            if link_match and link_match.group(1):
                key = re_key.match(link_match.group(1)).group(1)
                urls.append((int(key),
                             link_match.group(1).replace('//www.',
                                                         '//movies.')))
                #if len(urls) > 20:
                    #break

        master = {}
        def on_response(resp, prev_key=None, thread_urls=None,
                        thread_results=None, client=None):
            if resp is None:
                log.info('Starting thread')
            elif resp.error:
                log.error("Error: %s" % str(resp.error))
            else:
                for line in resp.buffer:
                    votes_match = re_votes.match(line)
                    if votes_match and votes_match.group(1):
                        thread_results[prev_key] = int(votes_match.group(1).replace(',', ''))
            try:
                next = thread_urls.next()
                client.fetch(next[1],
                             partial(on_response, prev_key=next[0],
                                     thread_urls=thread_urls,
                                     thread_results=thread_results,
                                     client=client),
                             headers=headers)
            except StopIteration:
                #if not tornado.ioloop.IOLoop.instance()._callbacks:
                    #tornado.ioloop.IOLoop.instance().stop()
                log.info('Done this thread (trailing key = %d)' % prev_key)
                start = True
                while start or cls._master_lock:
                    if not cls._master_lock:
                        cls._master_lock = True
                        master.update(thread_results)
                        cls._master_lock = False
                    start = False
                cls._thread_finished_count += 1
                if cls._thread_finished_count == cls._thread_count:
                    tornado.ioloop.IOLoop.instance().stop()
                    json.dump(master, open(config.netflix.votes_json_path, 'w'))

        ioloop = tornado.ioloop.IOLoop.instance()
        for i, url in enumerate(urls):
            if i < cls._thread_count:
                thread_urls = iter([ y for x, y in enumerate(urls) if
                                     x % cls._thread_count == i % cls._thread_count ])
                client = httpclient.AsyncHTTPClient()
                ioloop.add_callback(partial(on_response, None, None, thread_urls, {},
                                            client))
        ioloop.start()


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

            mpaa = format.find('./category[@sheme="http://api.netflix.com'
                               '/categories/mpaa_ratings"]')
            avail['mpaa'] = mpaa.get('label') if mpaa != None else None

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
            return avails
                
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
            return synopsis

        def _get_genres(elem):
            categories = elem.findall('./category')
            genres = []
            for cat in categories:
                scheme = cat.get('scheme')
                if 'genres' in scheme:
                    genres.append(clean(cat.get('label')))
            return genres

        def _get_people(elem):
            found = elem.findall('./people/link[@rel="http://schemas.netflix.com'
                                 '/catalog/person"]')
            people = []
            for i, person in enumerate(found):
                href = person.get('href')
                if not href:
                    continue
                id = int(href.rpartition('/')[2])
                name = person.get('title')
                if not name:
                    continue
                people.append({ 'key' : id, 'name' : name, 'billing' : i + 1 })
            return people

        def _get_cast(elem):
            schema = elem.find('./link[@rel="http://schemas.netflix.com'
                               '/catalog/people.cast"]')
            return _get_people(schema) if schema is not None else {}

        def _get_directors(elem):
            schema = elem.find('./link[@rel="http://schemas.netflix.com'
                               '/catalog/people.directors"]')
            return _get_people(schema) if schema is not None else {}

        def _form_title_dict(elem, key, votes=None):
            release_year = elem.find('release_year')
            if release_year == None or release_year.text == None:
                log.info('Year not found on %d' % key)
                return None
            link = elem.find('./link[@rel="http://schemas.netflix.com'
                             '/catalog/title/ref.tiny"]')
            href = { 'tiny' : link.get('href') } if link != None else {}

            title = {
                'key' : key,
                'name' : clean(elem.find('title').get('regular')), 
                'year' : int(release_year.text),
                'href' : href,
                'stat' : {
                    'votes' : votes,
                    'rating' : (Decimal(elem.find('average_rating').text) *
                                cls._rating_factor),
                },
                'synopsis' : _get_synopsis(elem),
                'availability' : _get_availabilities(elem),
                'art' : _get_art(elem),
                'genre' : _get_genres(elem),
                'production' : { 'director' : _get_directors(elem) },
                'cast' : _get_cast(elem),
            }

            return title

        if os.path.exists(config.netflix.votes_json_path):
            votes = json.load(open(config.netflix.votes_json_path))
        else:
            votes = {}

        context = etree.iterparse(cls._titles_file_path,
                                  events=('start', 'end'))
        context = iter(context)
        event, root = context.next()
        for event, elem in context:
            if event == 'end' and elem.tag == 'catalog_title':
                film_match = cls._re_film_test.match(elem.find('id').text)
                if film_match:
                    str_key = film_match.group(1)
                    if str_key in votes:
                        vote = votes[str_key]
                    else:
                        vote = None
                    title = _form_title_dict(elem, int(str_key), vote)
                    if title != None:
                        yield title
                elem.clear()
                root.clear()

if __name__ == '__main__':
    Fetch.fetch_data()
