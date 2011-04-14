import logging, re, time, HTMLParser, os, decimal
import oauth2 as oauth

from filmdata import config

log = logging.getLogger(__name__)

_source_max_rating = 5
_global_max_rating = int(config.get('core', 'max_rating'))
_rating_factor = _global_max_rating / _source_max_rating

_titles_file_path = config.get('netflix', 'titles_xml_path')
_titles_dir_path = config.get('netflix', 'titles_dir_path')

def _get_title_path(id):
    basename = '.'.join((id, 'xml'))
    bucket = id[:2]
    return os.path.join(_titles_dir_path, bucket, basename)


class Fetch:

    name = 'netflix'
    _consumer_key = config.get('netflix', 'consumer_key')
    _consumer_secret = config.get('netflix', 'consumer_secret')
    _titles_url = config.get('netflix', 'titles_url')
    _title_url_base = config.get('netflix', 'title_url_base')
    _call_limit = 4900

    @classmethod
    def fetch_data(this):
        this._download_title_catalog()
        this._download_titles()

    @classmethod
    def _download_title_catalog(this):
        if not os.path.isdir(os.path.dirname(_titles_file_path)):
            os.makedirs(os.path.dirname(_titles_file_path))
        xml = this._fetch(this._titles_url)
        f = open(_titles_file_path, 'w')
        f.write(xml)
        f.close()

    @classmethod
    def _download_titles(this):
        i = 0
        for title in Produce._get_titles():
            title_key = str(title[1][1]['key'])
            title_path = _get_title_path(title_key)
            if not os.path.isfile(title_path):
                dirname = os.path.dirname(title_path)
                if not os.path.isdir(dirname):
                    os.makedirs(dirname)
                title_url = os.path.join(this._title_url_base, title_key)
                xml = this._fetch(title_url)
                f = open(title_path, 'w')
                f.write(xml)
                f.close()

                i += 1
                if i > this._call_limit:
                    break

    @classmethod
    def _fetch(this, url):
        consumer = oauth.Consumer(key=this._consumer_key, secret=this._consumer_secret)
        client = oauth.Client(consumer)

        resp, content = client.request(url, "GET")
        if content == '<h1>403 Developer Over Rate</h1>':
            raise Exception('Over Netflix API rate limit')
        return content

class Produce:

    name = 'netflix'
    _re_item = re.compile('<title_index_item>(.*?)</title_index_item>', re.S)
    _re_title = re.compile('<title>(.*?)</title>')
    _re_numeric_id = re.compile('/([0-9]+)$')
    _re_title_rating = re.compile('<average_rating>([0-9]\.[0-9])</average_rating>')
    _re_tv_test = re.compile('<category.*? label="Television" term="Television">')

    _xml_fields = { 'id' : 'id', 'title' : 'name', 'release_year' : 'year' }

    @classmethod
    def produce_data(this, types):
        for title in this._get_titles(types):
            yield title

    @classmethod
    def _get_titles(this, types=None):
        in_item = False
        skip_title = False
        h = HTMLParser.HTMLParser()
        f = open(_titles_file_path, 'r')
        for line in f:
            line_clean = line.strip() 
            if not in_item and line_clean == '<title_index_item>':
                in_item = True
                title = { 'type' : 'film' }

            if in_item and not skip_title:
                if this._re_tv_test.match(line):
                    skip_title = True
                for field, title_key in this._xml_fields.iteritems():
                    field_len = len(field) + 2
                    if line_clean[:field_len] == '<%s>' % field:
                        slice_stop = line_clean.find('</%s>' % field)
                        value = line_clean[field_len:slice_stop]
                        title[title_key] = unicode(h.unescape(value))

            if line_clean == '</title_index_item>':
                in_item = False
                if skip_title:
                    skip_title = False
                    continue

                title_key = this._re_numeric_id.search(title['id']).group(1)
                del title['id']

                data = { 'rating' : this._get_title_rating(title_key),
                            'key' : int(title_key) }
                yield [ title, [ 'netflix', data ] ]
        f.close()

    @classmethod
    def _get_title_rating(this, id):
        path = _get_title_path(id)
        if os.path.isfile(path):
            f = open(path, 'r')
            xml = f.read()
            f.close()
            rating_match = this._re_title_rating.search(xml)
            rating = rating_match.group(1) if rating_match else 0
            return decimal.Decimal(title_info['rating']) * _rating_factor
        return 0

if __name__ == '__main__':
    Fetch.fetch_data()
