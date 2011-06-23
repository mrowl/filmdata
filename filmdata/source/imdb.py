import logging, re, os, decimal, md5

from filmdata import config

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

    _source_max_rating = 10
    _global_max_rating = int(config.core.max_rating)
    _rating_factor = _global_max_rating / _source_max_rating
    _re_title = re.compile('\s+([0-9.*]{10})\s+([0-9]+)\s+([0-9.]{3,4})\s+(.+)')
    _re_person_start = re.compile('^----\t\t\t------$')
    _re_character_role = re.compile('^(.+?)  (\[.+\])?\s*?(<[0-9]+>)?$')
    _re_person_name = re.compile('^(.*?)\t+(.*)$')
    _re_title_info = re.compile('(.+?)\s+\(([0-9]{4}|\?\?\?\?).*?\)\s?\(?(V|TV|VG)?.*$')
    _re_aka_title = re.compile('^\s+\(aka (.+?) \(([0-9]{4})\)\)\s+\((.+?)\)')

    @classmethod
    def produce_titles(cls, types):
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
                        rating = (decimal.Decimal(match.group(3)) *
                                  cls._rating_factor)
                        key = cls._ident_to_key(ident.encode('utf_8'))
                        title.update({
                            'ident' : ident,
                            'rating' : rating,
                            'votes' : int(match.group(2)),
                            'distribution' : match.group(1),
                            'key' : key,
                        })
                        yield title

    @classmethod
    def produce_roles(cls, title_types, role_types):
        def rname(n):
            a = n.split(',')
            return ' '.join(a[1:]).partition('(')[0].strip() + ' ' + a[0]
        clean_name = lambda x: re.sub('\(.*?\)', '', x)

        for role_type in role_types:
            person_ident = None

            type_path = config.imdb['%s_path' % role_type]
            log.info('Loading roles for "%s" from %s' % (role_type, type_path))
            f = open(type_path, 'r')
            while not cls._re_person_start.match(f.readline()):
                pass

            for line in f:
                title_ident = None
                if line[:9] == '---------':
                    log.info('End of File, done importing %s' % role_type)
                    break

                if not person_ident:
                    name_match = cls._re_person_name.match(line)
                    if name_match:
                        person_ident = name_match.group(1).decode('latin_1')
                        person_key = cls._ident_to_key(person_ident.encode('utf_8'))
                        role_string = name_match.group(2)
                elif not line.strip():
                    person_ident = None
                    continue
                else:
                    role_string = line.strip()

                role_string = role_string.decode('latin_1')
                role_match = cls._re_character_role.match(role_string)

                if role_match:
                    title_ident, character, billing = role_match.groups()
                    if character:
                        character = character.strip('[]')
                    else:
                        character = None
                    if billing:
                        billing = min(int(billing.strip('<>')), 32767)
                    else:
                        billing = None
                else:
                    log.debug("Errant line for person %s: %s" %
                              (person_ident, line.decode('latin_1')))
                    title_ident = role_string
                    character = billing = None

                if title_ident:
                    title = cls._parse_title_info(title_ident)
                else:
                    title = None

                if person_ident and title and title['type'] in title_types:
                    title_key = cls._ident_to_key(title_ident.encode('utf_8'))
                    title.update({
                        'key' : title_key,
                        'ident' : title_ident,
                    })
                    name = rname(clean_name(person_ident))
                    yield {
                        'title' : title,
                        'person' : {
                            'key' : person_key,
                            'ident' : person_ident,
                            'name' : name,
                        },
                        'role' : {
                            'type' : role_type,
                            'character' : character,
                            'billing' : billing
                        },
                    }

    @classmethod
    def produce_aka_titles(this, types):
        aka_path = config.imdb.aka_path
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
    def _ident_to_key(cls, ident):
        """ needs to be encoded to utf-8 """
        key_md5 = md5.new(ident)
        return key_md5.hexdigest()[:10]

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
                'name' : name,
                'year' : int(year),
                'type' : type }
        elif match:
            log.info("Title has unknown date, ignoring: %s" %
                     title_string)
        else:
            log.warn("Unable to parse title string %s" %
                     title_string)
        return None

if __name__ == '__main__':
    Fetch.fetch_data()
