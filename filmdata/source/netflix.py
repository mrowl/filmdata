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

class CatalogTitle:
    _re_award_cat = re.compile(' nominee$')

    def __init__(self, node, key, vote_count=None):
        self.node = node
        self.vote_count = vote_count
        self.key = key

    def get_title(self):
        release_year = self.node.find('release_year')
        if release_year == None or release_year.text == None:
            log.info('Year not found on %d' % self.key)
            return None
        link = self.node.find('./link[@rel="alternate"]')

        rating_text = self.node.find('average_rating').text.strip()
        rating = None if not rating_text else (Decimal(rating_text) *
                                               NetflixMixin._rating_factor)

        title = {
            'key' : self.key,
            'name' : Produce.sanitize_html(self.node.find('title').get('regular')), 
            'year' : int(release_year.text),
            'href' : link.get('href') if link != None else None,
            'type' : 'film',
            'rating' : {
                'user' : { 
                    'count' : self.vote_count,
                    'mean' : rating,
                },
            },
            'synopsis' : self._get_synopsis(),
            'availability' : self._get_availabilities(),
            'art' : self._get_art(),
            'genre' : self._get_genres(),
            'production' : { 'director' : self._get_directors() },
            'cast' : self._get_cast(),
            'award' : self._get_awards(),
        }
        title['runtime'] = self._get_runtime(title['availability'])

        return title
    
    def _get_runtime(self, availability):
        for medium in ('dvd', 'bluray', 'instant'):
            if availability.get(medium) and availability[medium].get('runtime'):
                return availability[medium]['runtime']
        return None

    def _get_availability(self, node):
        format = node.find('./category[@scheme='
                           '"http://api.netflix.com/categories/title_formats"]')
        if not format:
            log.warn('No format info found')
            return None

        avail = {
            'from' : node.get('available_from'),
            'until' : node.get('available_until'),
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
        runtime_node = node.find('runtime')
        if runtime_node != None:
            avail['runtime'] = int(round(float(runtime_node.text) / 60))
        else:
            avail['runtime'] = None

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

    def _get_availabilities(self):
        nodes = list(self.node.find('./link[@rel="http://schemas.'
                       'netflix.com/catalog/titles/f'
                       'ormat_availability"]/delivery'
                       '_formats'))
        avails = {}
        for avail in [ a for a in map(self._get_availability, nodes) if a ]:
            avails.update(avail)
        return avails
            
    def _get_art(self):
        box_art = self.node.find('./link[@rel="http://schemas.netflix.com'
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

    def _get_synopsis(self):
        synopsis = {}
        long = self.node.find('./link[@rel="http://schemas.netflix.com'
                         '/catalog/titles/synopsis"]/synopsis')
        if long != None:
            synopsis['long'] = Produce.sanitize_html(long.text)

        short = self.node.find('./link[@rel="http://schemas.netflix.com'
                          '/catalog/titles/synopsis.short"]/short_synopsis')
        if short != None:
            synopsis['short'] = Produce.sanitize_html(short.text)
        return synopsis

    def _get_genres(self):
        categories = self.node.findall('./category')
        genres = []
        for cat in categories:
            scheme = cat.get('scheme')
            if 'genres' in scheme:
                genres.append(Produce.sanitize_html(cat.get('label')))
        return genres

    def _get_people(self, node):
        found = node.findall('./people/link[@rel="http://schemas.netflix.com'
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

    def _get_cast(self):
        schema = self.node.find('./link[@rel="http://schemas.netflix.com'
                           '/catalog/people.cast"]')
        return self._get_people(schema) if schema is not None else {}

    def _get_directors(self):
        schema = self.node.find('./link[@rel="http://schemas.netflix.com'
                           '/catalog/people.directors"]')
        return self._get_people(schema) if schema is not None else {}

    def _get_award_info(self, node):
        category = node.find('category')
        if category == None:
            return None
        award = {}
        person = node.find('link')
        if (person != None and
            person.get('rel') ==
            'http://schemas.netflix.com/catalog/person'):
            award['person'] = {
                'key' : int(person.get('href').rpartition('/')[2]),
                'name' : person.get('title'),
            }
        award['name'] = category.get('scheme').rpartition('/')[2]
        award['category'] = CatalogTitle._re_award_cat.sub('', category.get('label'))
        award['year'] = node.get('year')
        return award

    def _get_awards(self):
        schema = self.node.find('./link[@rel="http://schemas.netflix.com'
                           '/catalog/titles/awards"]')
        if schema == None:
            return None
        awards_el = schema.find('awards')
        if awards_el == None:
            return None
        winners = awards_el.findall('award_winner')
        nominees = awards_el.findall('award_nominee')
        awards = {}
        for result, cats in (('won', winners), ('nominated', nominees)): 
            if cats != None:
                cat_list = filter(lambda c: c != None,
                                  map(self._get_award_info, cats))
                for cat in cat_list:
                    awards_name = cat['name']
                    awards_year = int(cat['year']) if cat['year'] else 0
                    del cat['name']
                    del cat['year']
                    if not awards_name in awards:
                        awards[awards_name] = {}
                    if not awards_year in awards[awards_name]:
                        awards[awards_name][awards_year] = {}
                    if not result in awards[awards_name][awards_year]:
                        awards[awards_name][awards_year][result] = []
                    awards[awards_name][awards_year][result].append(cat)
        return awards

class Produce(NetflixMixin):

    _re_film_test = re.compile('http://api.netflix.com/catalog/titles/movies/([0-9]+)')
    _h = HTMLParser.HTMLParser()

    @classmethod
    def sanitize_html(cls, x):
        return unicode(cls._h.unescape(x))

    @classmethod
    def produce_titles(cls, types):
        for title in cls._get_titles(types):
            yield title

    @classmethod
    def _get_titles(cls, types=None):

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
                    title = CatalogTitle(elem, int(str_key), vote).get_title()
                    #is_tv = title['genre'] and 'Television' in title['genre']
                    if title != None:
                        yield title
                elem.clear()
                root.clear()

if __name__ == '__main__':
    Fetch.fetch_data()
