import logging, re, os, decimal, md5, random

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

    _role_types = config.imdb.active_role_types.split()

    _source_max_rating = 10
    _global_max_rating = int(config.core.max_rating)
    _rating_factor = _global_max_rating / _source_max_rating
    _re_title = re.compile('\s+([0-9.*]{10})\s+([0-9]+)\s+([0-9.]{3,4})\s+(.+)')
    _re_title_info = re.compile('(.+?)\s+\(([0-9]{4}|\?\?\?\?).*?\)\s?\(?(V|TV|VG)?.*$')
    _re_aka_title = re.compile('^\s*\(aka (.+?) \(([0-9]{4})\)\)\s+\((.+?)\)\s*\(?([^)]+)?\)?')

    @classmethod
    def produce_titles(cls, types):
        titles = dict([ (s['key'], s) for
                        s in cls.produce_title_stats(types) ])
        valid_keys = frozenset(titles.keys())
        for genre in cls.produce_title_genres(types, keys=valid_keys):
            titles[genre['key']]['genre'] = genre['genre']
        for aka in cls.produce_title_akas(types, keys=valid_keys):
            titles[aka['key']]['aka'] = aka['aka']
        for mpaa in cls.produce_title_mpaas(types, keys=valid_keys):
            titles[mpaa['key']]['mpaa'] = mpaa['mpaa']
        for role in cls.produce_roles(types, keys=valid_keys):
            titles[role['key']]['cast'] = role['cast']
            titles[role['key']]['production'] = role['production']
        return titles.itervalues()

    @classmethod
    def produce_title_stats(cls, types, keys=None):
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
                            'key' : key,
                            'stat'  : {
                                'rating' : rating,
                                'votes' : int(match.group(2)),
                                'distribution' : match.group(1),
                            },
                        })
                        if keys is None or key in keys:
                            yield title

    @classmethod
    def produce_title_mpaas(cls, types, keys=None):
        re_mpaa = re.compile('^RE: Rated\s+(.*?)\s+(.*?)$')
        read_next = False
        for line in open(config.imdb.mpaa_path):
            if line[:4] == 'MV: ':
                ident = line[4:].strip().decode('latin_1')
                title = cls._parse_title_info(ident)
                if title and title['type'] in types:
                    title.update({
                        'key' : cls._ident_to_key(ident.encode('utf_8')),
                        'ident' : ident,
                    })
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
                if title is None or ident_new != title['ident']:
                    if (title is not None and
                        title['type'] in types and
                        (keys is None or title['key'] in keys)):
                        yield title
                    title = cls._parse_title_info(ident_new)
                    if title:
                        title.update({
                            'key' : cls._ident_to_key(ident_new.encode('utf_8')),
                            'ident' : ident_new,
                            'genre' : [],
                        })
                if title is not None:
                    title['genre'].append(match.group(2))

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
                    title = cls._parse_title_info(stripped)
                    if title and title['type'] not in types:
                        title = None
                    elif title:
                        title.update({
                            'key' : cls._ident_to_key(stripped.encode('utf_8')),
                            'ident' : stripped,
                            'aka' : [],
                        })
            else:
                match_aka = cls._re_aka_title.match(stripped)
                if match_aka:
                    note = match_aka.group(4) if match_aka.group(4) else None
                    title['aka'].append({
                        'name' : match_aka.group(1),
                        'year' : match_aka.group(2),
                        'region' : match_aka.group(3),
                        'note' : note,
                    })

    @classmethod
    def produce_roles(cls, title_types, keys=None):
        re_person_start = re.compile('^----\t\t\t------$')
        re_character_role = re.compile('^(.+?)  (\[.+\])?\s*?(<[0-9]+>)?$')
        re_person_name = re.compile('^(.*?)\t+(.*)$')
        re_writer_role = re.compile('^(.+?)  \((screenplay|written|original screenplay|original story|story).*?\)\s*?(<[0-9,]+>)?$')
        def rname(n):
            a = n.split(',')
            return ' '.join(a[1:]).partition('(')[0].strip() + ' ' + a[0]
        clean_name = lambda x: re.sub('\(.*?\)', '', x)
        titles = {}
        for role_type in cls._role_types:
            person_ident = None

            type_path = config.imdb['%s_path' % role_type]
            log.info('Loading roles for "%s" from %s' % (role_type, type_path))
            f = open(type_path, 'r')
            while not re_person_start.match(f.readline()):
                pass

            for line in f:
                title_ident = None
                if line[:9] == '---------':
                    log.info('End of File, done importing %s' % role_type)
                    break

                if not person_ident:
                    name_match = re_person_name.match(line)
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
                if role_type == 'writer':
                    role_match = re_writer_role.match(role_string)
                else:
                    role_match = re_character_role.match(role_string)

                if role_match:
                    title_ident, character, billing = role_match.groups()
                    if character:
                        character = character.strip('[]')
                    else:
                        character = None
                    if billing:
                        if role_type == 'writer':
                            billing = int(billing.strip('<>').partition(',')[0])
                        else:
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
                    title_key = cls._ident_to_key(title_ident.encode('utf_8'))
                else:
                    title = None

                if (person_ident and title and
                    title['type'] in title_types and
                    (keys is None or title_key in keys)):

                    name = rname(clean_name(person_ident))
                    if random.randint(0, 20000) == 0:
                        log.info('imdb %s status: %s' % (role_type, person_ident))

                    title['key'] = title_key
                    title['ident'] = title_ident
                    if not title_key in titles:
                        title['production'] = {}
                        for r in cls._role_types:
                            if r not in ('actor', 'actress'):
                                title['production'][r] = []
                        title['cast'] = []
                        titles[title_key] = title 
                    if role_type in ('actor', 'actress'):
                        titles[title_key]['cast'].append({
                            'name' : name,
                            'key' : person_key,
                            'ident' : person_ident,
                            'billing' : billing,
                            'character' : character,
                            'role' : role_type,
                        })
                    elif role_type == 'writer':
                        titles[title_key]['production'][role_type].append({
                            'name' : name,
                            'key' : person_key,
                            'ident' : person_ident,
                            'billing' : billing,
                            'role' : character,
                        })
                    else:
                        titles[title_key]['production'][role_type].append({
                            'name' : name,
                            'key' : person_key,
                            'ident' : person_ident,
                        })
        return titles.values()

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
            log.debug("Title has unknown date, ignoring: %s" %
                     title_string)
        else:
            log.warn("Unable to parse title string %s" %
                     title_string)
        return None

if __name__ == '__main__':
    Fetch.fetch_data()
