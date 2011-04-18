import logging, re, urllib, os, decimal
import sqlalchemy as sa

from filmdata import config

log = logging.getLogger(__name__)

class UnzipError(Exception): pass
class DownloadError(Exception): pass

_source_max_rating = 10
_global_max_rating = int(config.get('core', 'max_rating'))
_rating_factor = _global_max_rating / _source_max_rating

schema = {'votes' : 'integer'}

class Fetch:

    name = 'imdb'

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

    @staticmethod
    def _fetch(name):
        dest = config.get('imdb', '%s_path' % name)
        dest_dir = os.path.dirname(dest)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        url = config.get('imdb', '%s_url' % name)
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
    _re_title = re.compile('\s+[0-9.*]{10}\s+([0-9]+)\s+([0-9.]{3,4})\s+(.+)')
    _re_person_start = re.compile('^----\t\t\t------$')
    _re_character_role = re.compile('^(.+?)  (\[.+\])?\s*?(<[0-9]+>)?$')
    _re_person_name = re.compile('^(.*?)\t+(.*)$')
    _re_title_info = re.compile('(.+?)\s+\(([0-9]{4}|\?\?\?\?).*?\)\s?\(?(V|TV|VG)?.*$')
    _re_aka_title = re.compile('^\s+\(aka (.+?) \(([0-9]{4})\)\)\s+\((.+?)\)')

    @classmethod
    def produce_data(this, types):
        started = 0
        for line in open(config.get('imdb', 'rating_path')):
            if not started:
                started = 1 if line.strip() == 'MOVIE RATINGS REPORT' else 0
            else:
                match = this._re_title.match(line)
                if match:
                    title_key = this._parse_title_info(match.group(3))
                    if title_key and title_key['type'] in types:
                        rating = decimal.Decimal(match.group(2))
                        yield [ title_key,
                                [ 'imdb',
                                  { 'rating' : rating * _rating_factor,
                                    'votes' : int(match.group(1)) } ] ]

    @classmethod
    def produce_roles(this, title_types, role_types):
        for type in role_types:
            for role in this._get_roles(type, title_types):
                log.debug(role)
                yield role

    @classmethod
    def produce_aka_titles(this, types):
        aka_path = config.get('imdb', 'aka_path')
        log.info('Loading aka-titles from "%s"' % aka_path)
        f = open(aka_path, 'r')
        while not f.readline().strip() == 'AKA TITLES LIST':
            pass
        if f.readline().strip() == '===============':
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
                    title_key = this._parse_title_info(line)
                    if title_key and title_key['type'] not in types:
                        title_key = None
            else:
                match_aka = this._re_aka_title.match(line)
                if match_aka:
                    yield [ title_key,
                            { 'name' : match_aka.group(1).decode('latin_1'),
                              'year' : match_aka.group(2),
                              'region' : match_aka.group(3).decode('latin_1'), } ]

    @classmethod
    def _get_roles(this, type, title_types):
        person_name = None
        person_info = []

        type_path = config.get('imdb', '%s_path' % type)
        log.info('Loading roles for "%s" from %s' % (type, type_path))
        f = open(type_path, 'r')
        while not this._re_person_start.match(f.readline()):
            pass

        for line in f:
            title_string = None
            if line[:9] == '---------':
                log.info('End of File, done importing %s' % type)
                break

            if not person_name:
                name_match = this._re_person_name.match(line)
                if name_match:
                    person_name = name_match.group(1).decode('latin_1')
                    role_string = name_match.group(2)
            elif not line.strip():
                person_name = None
                continue
            else:
                role_string = line.strip()

            role_match = this._re_character_role.match(role_string)
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

            title_key = None
            if title_string:
                title_key = this._parse_title_info(title_string)

            if person_name and title_key and title_key['type'] in title_types:
                role = { 'type' : type,
                         'character' : character,
                         'billing' : billing }
                yield [ title_key, role, { 'name' : person_name } ]

    @classmethod
    def _parse_title_info(this, title_string):
        match = this._re_title_info.match(title_string.strip())
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
            return {
                'name' : name.decode('latin_1'),
                'year' : year,
                'type' : type }
        elif match:
            log.info("Title has unknown date, ignoring: %s" %
                     title_string.decode('latin_1'))
        else:
            log.warn("Unable to parse title string %s" %
                     title_string.decode('latin_1'))
        return None

if __name__ == '__main__':
    Fetch.fetch_data()
