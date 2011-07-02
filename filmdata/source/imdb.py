import logging
import itertools
import re
import os
import decimal
import hashlib
import random
from operator import itemgetter
from urllib import quote_plus
from functools import partial

from filmdata.lib.util import dson
from filmdata import config
from filmdata.lib.util import base_encode
from filmdata.lib.scrape import Scrape
import filmdata.sink

log = logging.getLogger(__name__)

class UnzipError(Exception): pass
class DownloadError(Exception): pass

schema = {
    'title_id' : 'id',
    'key' : 'varchar(32)',
    'rating' : None,
    'votes' : 'integer',
}

class Fetch:

    name = 'imdb'

    _id_data = {}
    _re_html_title_id = re.compile('<p><b>Titles (Exact Matches)</b>\s+'
                                   '(Displaying [0-9]+ Results?)<table><tr>\s+'
                                   '<td valign="top">'
                                   '<a href="/title/tt([0-9]+)/"')
    _re_uri_title_id = re.compile('^http://www.imdb.com/title/tt([0-9]+)/')
    _client = None

    @classmethod
    def fetch_data(this):
        this._fetch('rating')

    @classmethod
    def fetch_roles(this, roles):
        for role in roles:
            this._fetch(role)

    @classmethod
    def fetch_aka_titles(this):
        this._fetch('aka')

    @classmethod
    def fetch_ids(cls):
        scraper = Scrape(cls._get_title_urls(), cls._fetch_id_response,
                         scrape_callback=cls._scrape_response,
                         follow_redirects=False, max_clients=7, anon=True)
        scraper.run()
    
    @classmethod
    def _fetch_id_response(cls, resp, resp_url=None):
        id = None
        if resp is None:
            log.info('Starting thread')
        elif resp.error and getattr(resp.error, 'code', 999) < 400:
            uri = resp.error.response.headers['Location']
            uri_id_match = cls._re_uri_title_id.match(uri)
            if uri_id_match:
                id = int(uri_id_match.group(1))
                #print 'Uri id match: %s' % uri_id_match.group(1)
        elif resp.error:
            log.error("Scraper error:" % str(resp.error))
        else:
            print 'in html match'
            for line in resp.buffer:
                html_id_match = cls._re_html_title_id.search(line)
                if html_id_match:
                    #id = int(html_id_match.group(1))
                    print 'Html id match: %s' % html_id_match.group(1)
        if id:
            print id
            print resp_url[0]
            id_data = { 'ident' : resp_url[0] }
            filmdata.sink.store_source_data('imdb', data=id_data,
                                            id=id, suffix='title')

    @classmethod
    def _scrape_response(cls):
        log.info('Done scraping! Dumping now...')
        dson.dump(cls._get_known_ids(), config.imdb.title_ids_path)

    @classmethod
    def _get_known_ids(cls):
        return dict([ (x['ident'], x['id']) for x in
                      filmdata.sink.get_source_data('imdb', 'title') ])

    @classmethod
    def _get_title_urls(cls, only_new=True):
        iterator = itertools.imap(lambda i: (i, Produce._title_href(i)),
                                  Produce.produce_title_stats(('film'),
                                                      idents_only=True))
        if only_new:
            known_ids = cls._get_known_ids()
            return itertools.ifilter(lambda u: u[0] not in known_ids,
                                     iterator)
        return iterator

    @staticmethod
    def _fetch(name):
        dest = config.imdb['%s_path' % name]
        dest_dir = os.path.dirname(dest)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        url = config.imdb['%s_url' % name]
        dest_gz = os.path.join(dest_dir, os.path.basename(url))
        cmd = 'wget -O %s %s' % (dest_gz, url)
        ret = os.system(cmd)
        if ret > 0 or not os.access(dest_gz, os.R_OK):
            raise DownloadError('Error downloading file: %s' % cmd)

        cmd = 'gunzip -c %s > %s' % (dest_gz, dest)
        ret = os.system(cmd)
        if ret > 0 or not os.access(dest, os.R_OK):
            raise UnzipError('Error gunzipping file: %s' % cmd)

        ret = os.system('rm %s' % dest_gz)
        if ret > 0 or os.access(dest_gz, os.R_OK):
            raise UnzipError('Unable to remove the archive')

class Produce:

    name = 'imdb'

    _role_types = config.imdb.active_role_types.split()

    _source_max_rating = 10
    _global_max_rating = int(config.core.max_rating)
    _rating_factor = _global_max_rating / _source_max_rating
    _re_title = re.compile('\s+([0-9.*]{10})\s+([0-9]+)\s+([0-9.]{3,4})\s+(.+)')
    _re_title_info = re.compile('(.+?)\s+\(([0-9]{4}|\?\?\?\?).*?\)\s?\(?(V|TV|VG)?.*$')
    _re_aka_title = re.compile('^\s*\(aka (.+?) \(([0-9]{4})\)\)\s+\((.+?)\)\s*\(?([^)]+)?\)?')

    @classmethod
    def produce_titles(cls, types):
        #cls.produce_roles(types, group='cast')

        titles = dict([ (s['key'], s) for
                        s in cls.produce_title_stats(types) ])


        aux_producers = {
            'runtime' : cls.produce_title_runtimes,
            'cast' : partial(cls.produce_roles, group='cast'),
            'director' : partial(cls.produce_roles, group='director'),
            'writer' : partial(cls.produce_roles, group='writer'),
            'genre' : cls.produce_title_genres,
            'aka' : cls.produce_title_akas,
            'mpaa' : cls.produce_title_mpaas,
        }

        valid_keys = frozenset(titles.keys())
        for name, func in aux_producers.items():
            for title in func(types, keys=valid_keys):
                titles[title['key']][name] = title[name]
        return titles.itervalues()

    @classmethod
    def produce_title_stats(cls, types, keys=None, idents_only=False):
        started = 0
        for line in open(config.imdb.rating_path):
            if not started:
                started = 1 if line.strip() == 'MOVIE RATINGS REPORT' else 0
            else:
                match = cls._re_title.match(line.decode('latin_1'))
                if match:
                    ident = match.group(4)
                    title = cls._parse_title_info(ident)
                    if title and title['type'] in types:
                        if idents_only:
                            yield ident
                        else:
                            rating = (decimal.Decimal(match.group(3)) *
                                      cls._rating_factor)
                            title.update({
                                'rating'  : {
                                    'mean' : rating,
                                    'count' : int(match.group(2)),
                                    'distribution' : match.group(1),
                                },
                            })
                            if keys is None or title['key'] in keys:
                                yield title

    @classmethod
    def produce_title_mpaas(cls, types, keys=None):
        re_mpaa = re.compile('^RE: Rated\s+(.*?)\s+(.*?)$')
        read_next = False
        for line in open(config.imdb.mpaa_path):
            if line[:4] == 'MV: ':
                ident = line[4:].strip().decode('latin_1')
                title = cls._parse_title_info(ident, add_info=False)
                if title and title['type'] in types:
                    read_next = True
            elif line[:4] == 'RE: ' and read_next:
                line_clean = line.strip().decode('latin_1')
                match = re_mpaa.match(line_clean)
                if match and match.group(1):
                    title['mpaa'] = {
                        'rating' : match.group(1),
                        'reason' : match.group(2),
                    }
                elif 'mpaa' in title:
                    title['mpaa']['reason'] += ' ' + line_clean[4:]
            elif not line.strip() and read_next:
                read_next = False
                if 'mpaa' in title and (keys is None or title['key'] in keys):
                    yield title

    @classmethod
    def produce_title_runtimes(cls, types, keys=None):
        re_runtime = re.compile('^(.*?)\t+.*?([0-9]+)')
        f = open(config.imdb.runtime_path, 'r')
        while not f.readline().strip() == '==================':
            pass
        for line in f:
            line_clean = line.strip().decode('latin_1')
            match = re_runtime.match(line_clean)
            if match and match.group(1) and match.group(2):
                ident = match.group(1)
                title = cls._parse_title_info(ident, add_info=False)
                if (title is not None and title['type'] in types and
                    (keys is None or title['key'] in keys)):
                    title['runtime'] = int(match.group(2))
                    yield title
        f.close()

    @classmethod
    def produce_title_genres(cls, types, keys=None):
        re_genre = re.compile('^(.*?)\t+(.*?)$')
        f = open(config.imdb.genre_path, 'r')
        while not f.readline().strip() == '8: THE GENRES LIST':
            pass
        if f.readline().strip() == '==================':
            pass
        if f.readline().strip() == '':
            pass
        title = None
        for line in f:
            line_clean = line.strip().decode('latin_1')
            match = re_genre.match(line_clean)
            if match and match.group(1) and match.group(2):
                ident_new = match.group(1)
                key_new = cls._ident_to_key(ident_new.encode('utf_8'))
                if title is None or key_new != title['key']:
                    if (title is not None and
                        title['type'] in types and
                        (keys is None or title['key'] in keys)):
                        yield title
                    title = cls._parse_title_info(ident_new, add_info=False)
                    if title:
                        title['genre'] = []
                if title is not None:
                    title['genre'].append(match.group(2))
        f.close()

    @classmethod
    def produce_title_akas(cls, types, keys=None):
        aka_path = config.imdb.aka_path
        log.info('Loading aka-titles from "%s"' % aka_path)
        f = open(aka_path, 'r')
        while not f.readline().strip() == 'AKA TITLES LIST':
            pass
        if f.readline().strip() == '===============':
            pass

        title = None

        for line in f:
            if line[:9] == '---------':
                log.info('End of File, done importing aka-titles')
                break

            stripped = line.strip().decode('latin_1')
            if not stripped:
                if title and (keys is None or title['key'] in keys):
                    yield title
                title = None
            elif not title:
                if not stripped[:4] == '(aka':
                    title = cls._parse_title_info(stripped, add_info=False)
                    if title and title['type'] not in types:
                        title = None
                    elif title:
                        title['aka'] = []
            else:
                match_aka = cls._re_aka_title.match(stripped)
                if match_aka:
                    note = match_aka.group(4) if match_aka.group(4) else None
                    title['aka'].append({
                        'name' : match_aka.group(1),
                        'year' : int(match_aka.group(2)),
                        'region' : match_aka.group(3),
                        'note' : note,
                    })
        f.close()

    @classmethod
    def produce_roles(cls, title_types, group=None, keys=None):
        re_person_start = re.compile('^----\t\t\t------$')
        re_character_role = re.compile('^(.+?)  (\[.+\])?\s*?(<[0-9]+>)?$')
        re_person_name = re.compile('^(.*?)\t+(.*)$')
        re_writer_role = re.compile('^(.+?)  \((screenplay|written|original screenplay|original story|story).*?\)\s*?(<[0-9,]+>)?$')
        role_matcher = re_writer_role if group == 'writer' else re_character_role

        def rname(n):
            a = n.split(',')
            return ' '.join(a[1:]).partition('(')[0].strip() + ' ' + a[0]
        clean_name = lambda x: re.sub('\(.*?\)', '', x)
        titles = {}
        cast_set = set(('actor', 'actress'))
        if group == 'writer':
            role_types = set(cls._role_types) & set(('writer', ))
        elif group == 'director':
            role_types = set(cls._role_types) & set(('director', ))
        elif group == 'cast':
            role_types = set(cls._role_types) & cast_set
        else:
            return []

        for role_type in role_types:
            person_ident = None

            type_path = config.imdb['%s_path' % role_type]
            log.info('Loading roles for "%s" from %s' % (role_type, type_path))
            f = open(type_path, 'r')

            while not re_person_start.match(f.readline()):
                pass

            for line in itertools.imap(lambda l: l.strip().decode('latin_1'), f):
                title_ident = None
                if line[:9] == '---------':
                    log.info('End of File, done importing %s' % role_type)
                    break

                if not person_ident:
                    name_match = re_person_name.match(line)
                    if name_match:
                        person_ident = name_match.group(1)
                        person_key = cls._ident_to_key(person_ident.encode('utf_8'))
                        role_string = name_match.group(2)
                elif not line:
                    person_ident = None
                    continue
                else:
                    role_string = line

                role_match = role_matcher.match(role_string)

                if role_match:
                    title_ident, character, billing = role_match.groups()
                    character = character.strip('[]') if character else None
                    if billing:
                        billing = billing.strip('<>')
                        if role_type == 'writer':
                            billing = int(billing.partition(',')[0])
                        else:
                            billing = min(int(billing), 32767)
                else:
                    log.debug("Errant line for person %s: %s" %
                              (person_ident, line))
                    title_ident = role_string
                    character = billing = None

                if title_ident:
                    title = cls._parse_title_info(title_ident, add_info=False)
                else:
                    title = None

                if (person_ident and title and
                    title['type'] in title_types and
                    (keys is None or title['key'] in keys)):

                    name = rname(clean_name(person_ident))
                    if random.randint(0, 20000) == 0:
                        log.info('imdb %s status: %s' % (role_type, person_ident))

                    if not title['key'] in titles:
                        del title['type']
                        titles[title['key']] = title 
                        titles[title['key']][group] = []

                    person = {
                        'name' : name,
                        'key' : person_key,
                        'href' : cls._person_href(person_ident),
                    }
                    if group == 'writer' or group == 'cast':
                        person['billing'] = billing
                        if group == 'cast':
                            person['character'] = character
                            person['role'] = role_type
                        if role_type == 'writer':
                            person['role'] = character
                    titles[title['key']][group].append(person)
                    #print "\n"
                    #print title['key']
                    #print titles[title['key']][group]
                    #print "\n"

            f.close()
        for title_key in titles.keys():
            for billing_type in ('cast', 'writer'):
                if titles[title_key].get(billing_type):
                    titles[title_key][billing_type].sort(key=itemgetter('billing'))
        return titles.values()

    @classmethod
    def _title_href(cls, ident):
        return 'http://www.imdb.com/find?s=tt&q=%s' % cls._super_quote(ident)

    @classmethod
    def _person_href(cls, ident):
        return 'http://www.imdb.com/find?s=nm&q=%s' % cls._super_quote(ident)

    @classmethod
    def _super_quote(cls, s):
        new_s = []
        for c in s:
            ord_val = ord(c)
            if (ord_val > 127):
                new_s.append('%' + str(hex(ord_val)).lstrip('0x').upper())
            else:
                new_s.append(quote_plus(c))
        return ''.join(new_s)

    @classmethod
    def _ident_to_key(cls, ident):
        """ needs to be encoded to utf-8 """
        return base_encode(int(hashlib.md5(ident).hexdigest()[:14], 16), 62)

    @classmethod
    def _parse_title_info(cls, ident, add_info=True):
        match = cls._re_title_info.match(ident.strip())
        if match and match.group(2) != '????':
            name = match.group(1)
            year = match.group(2)
            if (name[0] == '"' and name[-1] == '"') or match.group(3) == 'TV':
                name = name.strip('"')
                type = 'tv'
            elif match.group(3) == 'V':
                type = 'video'
            elif match.group(3) == 'VG':
                type = 'game'
            else:
                type = 'film'
            title = {
                'key' : cls._ident_to_key(ident.encode('utf_8')),
                'type' : type
            }
            if add_info:
                title['href'] = cls._title_href(ident)
                title['name'] = name
                title['year'] = int(year)
            return title
        elif match:
            log.debug("Title has unknown date, ignoring: %s" % ident)
        else:
            log.warn("Unable to parse title string %s" % ident)
        return None

if __name__ == '__main__':
    Fetch.fetch_data()
