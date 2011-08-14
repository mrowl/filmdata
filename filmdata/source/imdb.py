import logging
import re
import os
import decimal
import hashlib
from HTMLParser import HTMLParser
from operator import itemgetter
from urllib import quote_plus
from functools import partial
from itertools import imap, ifilter

from filmdata.lib.util import dson, rname, clean_name
from filmdata.lib.util import class_property, extract_name_suffix
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

class ImdbMixin:

    name = 'imdb'

    _role_types = frozenset(config.core.active_role_types.split())
    _group_role_types = {
        'writer' : set(('writer', )),
        'director' : set(('director', )),
        'cast' : set(('actor', 'actress')),
    }
    _billing_groups = frozenset(('writer', 'cast'))
    _title_types = frozenset(config.core.active_title_types.split())

    @classmethod
    def _get_known_ids(cls, type='title'):
        return dict([ (x['ident'], x['id']) for x in
                      filmdata.sink.get_source_data('imdb', type) ])

class Fetch(ImdbMixin):

    _re_html_title_id = re.compile('<p><b>Titles (Exact Matches)</b>\s+'
                                   '(Displaying [0-9]+ Results?)<table><tr>\s+'
                                   '<td valign="top">'
                                   '<a href="/title/tt([0-9]+)/"')
    #_popular_id_string = '^\s*<p><b>Popular Names</b>.*?<br><a href="/name/nm([0-9]+)/" onclick="[^"]">%s</a>\s*<small>'
    #_partial_id_string = '<a href="/name/nm([0-9]+)/" onclick="[^"]+">%s</a>\s*(:?%s)?\s*<small>'
    _person_id_string = '<a href="/name/nm([0-9]+)/" onclick="[^"]+">%s</a>\s*%s<small>'
    _re_uri_id = re.compile('^http://www.imdb.com/(title|name)/(nm|tt)([0-9]+)/?')

    @classmethod
    def fetch_data(cls):
        cls._fetch('rating')
        cls._fetch('aka')
        cls._fetch('runtime')
        cls._fetch('genre')
        cls._fetch('mpaa')
        for role in cls._role_types:
            cls._fetch(role)
        for thing_type in ('title', 'person'):
            cls.fetch_ids(cls._title_types, thing_type)

    @classmethod
    def fetch_ids(cls, title_types, type='title'):
        cls._type = type
        if type == 'title':
            url_source = cls._get_title_urls
        else:
            url_source = cls._get_person_urls
        #url_source = lambda t: iter([('Prowse, David', Produce._person_href(None, ident='Prowse, David'))])
        scraper = Scrape(url_source(title_types), cls._fetch_id_response,
                         follow_redirects=False, max_clients=8,
                         delay=1, anon=True)
        scraper.run()
        #cls._scrape_response(type=type)
    
    @classmethod
    def _fetch_id_response(cls, resp, resp_url=None):
        id = None
        ident = resp_url[0]
        if resp_url[1] != resp.effective_url:
            log.warning('Fetched url and effective url do not match')
            log.warning('%s  ;  %s' % (str(resp_url), resp.effective_url))
            return None

        if resp.status >= 300 and resp.status < 400:
            uri = resp.location
            uri_id_match = cls._re_uri_id.match(uri)
            if uri_id_match:
                id = int(uri_id_match.group(3))
                print 'uri matched %s %s to %s' % (cls._type, ident, str(id))
            else:
                print 'redirect with no uri id match: %s' % uri
        elif resp.status >= 400:
            log.error("Scraper error:" % str(resp))
        elif cls._type == 'person':
            id = cls._extract_id_from_html(resp.buffer.split("\n"), ident)
            if id:
                print 'html matched %s %s to %s' % (cls._type, ident, str(id))
            else:
                print 'no match for %s %s' % (cls._type, ident)
        if id:
            id_data = { 'ident' : ident }
            filmdata.sink.store_source_data('imdb', data=id_data,
                                            id=id, suffix=cls._type)
    
    @classmethod
    def _extract_id_from_html(cls, lines, ident):
        h = HTMLParser()
        suffix = extract_name_suffix(ident)
        if suffix:
            suffix = suffix.replace('(', '\(', 1).replace(')', '\)', 1)
            suffix = '(:?%s)?\s*' % suffix
        person_id_string = cls._person_id_string % (rname(clean_name(ident)),
                                                    suffix) 
        re_person_id = re.compile(person_id_string, re.I)
        for line in imap(h.unescape, lines):
            id_match = re_person_id.search(line)
            if id_match:
                return int(id_match.group(1))
        return None

    @classmethod
    def _scrape_response(cls, type='title'):
        log.info('Done scraping! Dumping now...')
        dson.dump(cls._get_known_ids(type), config.imdb['%s_ids_path' % type])

    @classmethod
    def _get_title_urls(cls, title_types, only_new=True):
        iterator = imap(lambda i: (i, Produce._title_href(None, ident=i)),
                                  Produce.produce_title_stats(title_types,
                                                      idents_only=True))
        if only_new:
            known_ids = cls._get_known_ids('title')
            return ifilter(lambda u: u[0] not in known_ids,
                                     iterator)
        return iterator

    @classmethod
    def _get_person_urls(cls, title_types, only_new=True):
        def gen():
            persons = set()
            for role_type in cls._role_types:
                for ident in Produce.produce_persons(role_type,
                                                     idents_only=True):
                    if not ident in persons:
                        href = Produce._person_href(None, ident=ident)
                        persons.add(ident)
                        yield (ident, href)

        if only_new:
            known_ids = cls._get_known_ids('person')
            return ifilter(lambda u: u[0] not in known_ids, gen())

        return gen()

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

class Produce(ImdbMixin):

    _source_max_rating = 10
    _global_max_rating = int(config.core.max_rating)
    _rating_factor = _global_max_rating / _source_max_rating
    _re_title = re.compile('\s+([0-9.*]{10})\s+([0-9]+)\s+([0-9.]{3,4})\s+(.+)')
    _re_title_info = re.compile('(.+?)\s+\(([0-9]{4}|\?\?\?\?).*?\)\s?\(?(V|TV|VG)?.*$')
    _re_aka_title = re.compile('^\s*\(aka (.+?) \(([0-9]{4})\)\)\s+\((.+?)\)\s*\(?([^)]+)?\)?')
    _re_character_role = re.compile('^(.+?)(?:  \(as .+?\))?(?:  )?(\[.+\])?\s*?(<[0-9]+>)?$')
    _re_writer_role = re.compile('^(.+?)  \((screenplay|written|original screenplay|original story|story).*?\)\s*?(<[0-9,]+>)?$')

    @class_property
    @classmethod
    def title_producers(cls):
        if not hasattr(cls, '_title_producers'):
            cls._title_producers = {
                'rating' : cls.produce_title_stats,
                'runtime' : cls.produce_title_runtimes,
                'genre' : cls.produce_title_genres,
                'aka' : cls.produce_title_akas,
                'mpaa' : cls.produce_title_mpaas,
            }
        return cls._title_producers

    @class_property
    @classmethod
    def role_producers(cls):
        if not hasattr(cls, '_role_producers'):
            cls._role_producers = {
                'cast' : partial(cls.produce_title_roles, group='cast'),
                'director' : partial(cls.produce_title_roles,
                                     group='director'),
                'writer' : partial(cls.produce_title_roles, group='writer'),
            }
        return cls._role_producers

    @class_property
    @classmethod
    def title_ident_to_id(cls):
        if not hasattr(cls, '_title_ident_to_id'):
            cls._title_ident_to_id = cls._get_known_ids('title')
        return cls._title_ident_to_id

    @class_property
    @classmethod
    def person_ident_to_id(cls):
        if not hasattr(cls, '_person_ident_to_id'):
            cls._person_ident_to_id = cls._get_known_ids('person')
        return cls._person_ident_to_id

    @classmethod
    def produce_titles(cls, types, roles_only=False):
        producers = cls.role_producers.copy()
        if not roles_only:
            producers.update(cls.title_producers)

        titles = {}
        for name, func in producers.items():
            for title in func(types):
                id = title['id']
                if not id in titles:
                    titles[id] = title
                else:
                    titles[id].update(title)
        return titles.itervalues()
    
    @classmethod
    def produce_title_roles(cls, title_types, group=None):
        titles = {}
        for role_type in cls._group_role_types[group]:
            for person in cls.produce_persons(role_type):
                for role in person['roles']:
                    title_id = role['title_id']
                    del role['title_id']
                    role['person_id'] = person['id']
                    role['name'] = person['name']
                    if not title_id in titles:
                        titles[title_id] = { 'id' : title_id } 
                        titles[title_id][group] = []
                    titles[title_id][group].append(role)

        for title_key in titles.keys():
            for billing_group in cls._billing_groups:
                if titles[title_key].get(billing_group):
                    titles[title_key][billing_group].sort(key=itemgetter('billing'))
        return titles.values()

    @classmethod
    def produce_persons(cls, role_type, idents_only=False, sans_roles=False):
        re_person_start = re.compile('^----\t\t\t------$')
        re_person_name = re.compile('^(.*?)\t+(.*)$')

        type_path = config.imdb['%s_path' % role_type]
        log.info('Loading roles for "%s" from %s' % (role_type, type_path))
        f = open(type_path, 'r')

        while not re_person_start.match(f.readline()):
            pass

        person_new = lambda: { 'name' : None, 'id' : None, 'roles' : [] }
        person_ident, person = None, person_new()
        for line in imap(lambda l: l.strip().decode('latin_1'), f):
            if line[:9] == '---------':
                log.info('End of File, done importing %s' % role_type)
                break

            if not person_ident:
                name_match = re_person_name.match(line)
                if name_match:
                    person_ident = name_match.group(1)
                    person['name'] = rname(clean_name(person_ident))
                    role_ident = name_match.group(2)
            elif not line:
                if person_ident and person['roles']:
                    if idents_only:
                        yield person_ident
                    else:
                        person['id'] = cls.person_ident_to_id.get(person_ident)
                        person['href'] = cls._person_href(person['id'],
                                                          ident=person_ident)
                        if sans_roles:
                            del person['roles']
                        yield person
                person_ident, person = None, person_new()
                continue
            else:
                role_ident = line

            role = cls._parse_role_ident(role_ident, role_type)
            if role['title_ident'] in cls.title_ident_to_id:
                role['title_id'] = cls.title_ident_to_id[role['title_ident']]
                del role['title_ident']
                person['roles'].append(role)
        f.close()

    @classmethod
    def _parse_role_ident(cls, ident, role_type):
        role = {
            'title_ident' : ident,
            'billing' : None,
        }
        if role_type == 'writer':
            role_matcher = cls._re_writer_role
        else:
            role_matcher = cls._re_character_role

        role_match = role_matcher.match(ident)
        if not role_match:
            return role

        title_ident, character, billing = role_match.groups()
        character = character.strip('[]') if character else None
        billing = billing.strip('<>') if billing else None
        if role_type == 'writer':
            if billing:
                role['billing'] = int(billing.partition(',')[0])
            role['role'] = character
        elif role_type in cls._group_role_types['cast']:
            role['character'] = character
            role['role'] = role_type
            if billing:
                role['billing'] = min(int(billing), 32767)
        role['title_ident'] = title_ident
        return role

    @classmethod
    def produce_title_stats(cls, types, idents_only=False):
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
                            if ident in cls.title_ident_to_id:
                                rating = (decimal.Decimal(match.group(3)) *
                                          cls._rating_factor)
                                title.update({
                                    'rating'  : {
                                        'mean' : rating,
                                        'count' : int(match.group(2)),
                                        'distribution' : match.group(1),
                                    },
                                    'id' : cls.title_ident_to_id[ident]
                                })
                                title['href'] = cls._title_href(title['id'])
                                yield title

    @classmethod
    def produce_title_mpaas(cls, types):
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
                if 'mpaa' in title and ident in cls.title_ident_to_id:
                    title['id'] = cls.title_ident_to_id[ident]
                    yield title

    @classmethod
    def produce_title_runtimes(cls, types):
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
                    (ident in cls.title_ident_to_id)):
                    title['runtime'] = int(match.group(2))
                    title['id'] = cls.title_ident_to_id[ident]
                    yield title
        f.close()

    @classmethod
    def produce_title_genres(cls, types):
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
                if title is None or ident_new != title['ident']:
                    if (title is not None and
                        title['type'] in types and
                        title['ident'] in cls.title_ident_to_id):
                        title['id'] = cls.title_ident_to_id[title['ident']]
                        yield title
                    title = cls._parse_title_info(ident_new, add_info=False)
                    if title:
                        title['genre'] = []
                if title is not None:
                    title['genre'].append(match.group(2))
        f.close()

    @classmethod
    def produce_title_akas(cls, types):
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
                if title and (title['ident'] in cls.title_ident_to_id):
                    title['id'] = cls.title_ident_to_id[title['ident']]
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
    def _title_href(cls, id, ident=None):
        if not id and ident:
            return 'http://www.imdb.com/find?s=tt&q=%s' % cls._super_quote(ident)
        return 'http://www.imdb.com/title/tt%s/' % str(id).rjust(7, '0')

    @classmethod
    def _person_href(cls, id, ident=None):
        if not id and ident:
            return 'http://www.imdb.com/find?s=nm&q=%s' % cls._super_quote(ident)
        return 'http://www.imdb.com/name/nm%s/' % str(id).rjust(7, '0')

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
                'ident' : ident,
                'type' : type,
            }
            if add_info:
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
