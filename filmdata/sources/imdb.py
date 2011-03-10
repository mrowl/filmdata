import re, urllib, os, logging, decimal
import warnings

from filmdata import config

log = logging.getLogger(__name__)

#warnings.simplefilter('error')

class ImdbSource:

    name = 'imdb'

    __re_title = re.compile('\s+[0-9.*]{10}\s+([0-9]+)\s+([0-9.]{3,4})\s+(.+)')
    __re_person_start = re.compile('^----\t\t\t------$')
    __re_character_role = re.compile('^(.+?)  (\[.+\])?\s*?(<[0-9]+>)?$')
    __re_person_name = re.compile('^(.*?)\t+(.*)$')
    __re_title_info = re.compile('(.+?)\s+\(([0-9]{4}).*?\)\s?\(?(V|TV|VG)?.*$')
    __re_aka_title = re.compile('^\s+\(aka (.+?) \(([0-9]{4})\)\)\s+\((.+?)\)')

    def __init__(self):
        pass

    def fetch_data(self):
        self.__fetch('rating')
    
    def fetch_roles(self, roles):
        for role in roles:
            self.__fetch(role)

    def fetch_aka_titles(self):
        self.__fetch('aka')

    def produce_roles(self, title_types, role_types):
        for type in role_types:
            for role in self.__get_roles(type, title_types):
                log.debug(role)
                yield role

    def produce_numbers(self, types):
        started = 0
        for line in open(config.get('imdb', 'rating_path')):
            if not started:
                started = 1 if line.strip() == 'MOVIE RATINGS REPORT' else 0
            else:
                match = self.__re_title.match(line)
                if match:
                    title_key = self.__parse_title_info(match.group(3))
                    if title_key and title_key['type'] in types:
                        yield [ title_key,
                                [ 'imdb',
                                  { 'rating' : decimal.Decimal(match.group(2)),
                                    'votes' : int(match.group(1)) } ] ]

    def produce_aka_titles(self, types):
        aka_path = config.get('imdb', 'aka_path')
        log.info('Loading aka-titles from "%s"' % aka_path)
        f = open(aka_path, 'r')
        while not f.readline().strip() == 'AKA TITLES LIST':
            pass

        title_key = None

        for line in f:
            if line[:9] == '---------':
                log.info('End of File, done importing aka-titles')
                break

            stripped = line.strip()
            if not stripped:
                title_key = None
            elif not title_key:
                if not stripped[:4] == '(aka':
                    title_key = self.__parse_title_info(line)
                    if title_key and title_key['type'] not in types:
                        title_key = None
            else:
                match_aka = self.__re_aka_title.match(line)
                if match_aka:
                    yield [ title_key,
                            { 'name' : match_aka.group(1).decode('latin_1'),
                              'year' : match_aka.group(2),
                              'region' : match_aka.group(3).decode('latin_1'), } ]

    def __get_roles(self, type, title_types):
        person_name = None
        person_info = []

        type_path = config.get('imdb', '%s_path' % type)
        log.info('Loading roles for "%s" from %s' % (type, type_path))
        f = open(type_path, 'r')
        while not self.__re_person_start.match(f.readline()):
            pass

        for line in f:
            title_string = None
            if line[:9] == '---------':
                log.info('End of File, done importing %s' % type)
                break

            if not person_name:
                name_match = self.__re_person_name.match(line)
                if name_match:
                    person_name = name_match.group(1).decode('latin_1')
                    role_string = name_match.group(2)
            elif not line.strip():
                person_name = None
                continue
            else:
                role_string = line.strip()

            role_match = self.__re_character_role.match(role_string)
            if role_match:
                title_string, character, billing = role_match.groups()
                if character:
                    character = character.strip('[]').decode('latin_1')
                if billing:
                    billing = billing.strip('<>')
            else:
                log.debug("Errant line for person %s: %s" %
                          (person_name, line.decode('latin_1')))
                title_string = role_string
                character = billing = None

            title_key = self.__parse_title_info(title_string) if title_string else None

            if person_name and title_key and title_key['type'] in title_types:
                role = { 'type' : type,
                         'character' : character,
                         'billing' : billing }
                yield [ title_key, role, { 'name' : person_name } ]

    def __parse_title_info(self, title_string):
        match = self.__re_title_info.match(title_string.strip())
        if match:
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
            return {
                'name' : name.decode('latin_1'),
                'year' : year,
                'type' : type }

        log.warn("Unable to parse title string %s" % title_string.decode('latin_1'))
        return None

    def __fetch(self, name):
        dest = config.get('imdb', '%s_path' % name)
        url = config.get('imdb', '%s_url' % name)
        dest_tmp = os.path.basename(url)
        ret = os.system('wget -O %s %s' % (dest_tmp, url))
        if ret > 0:
            raise Exception('Error downloading file %s to %s' % (url, dest))
        ret = os.system('gunzip -c %s > %s' % (dest_tmp, dest))
        if ret > 0:
            raise Exception('Error gunzipping file %s' % dest_tmp)
