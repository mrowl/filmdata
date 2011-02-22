import re, time, warnings, HTMLParser, os, decimal
import oauth2 as oauth
from filmdata import config

warnings.simplefilter('error')

class NetflixSource:

    name = 'netflix'

    __titles_file = config.get('netflix', 'titles_xml_path')
    __titles_folder = config.get('netflix', 'titles_dir_path')
    __consumer_key = config.get('netflix', 'consumer_key')
    __consumer_secret = config.get('netflix', 'consumer_secret')
    __titles_url = config.get('netflix', 'titles_url')
    __title_url_base = config.get('netflix', 'title_url_base')

    __re_item = re.compile('<title_index_item>(.*?)</title_index_item>', re.S)
    __re_title = re.compile('<title>(.*?)</title>')
    __re_numeric_id = re.compile('/([0-9]+)$')
    __re_title_rating = re.compile('<average_rating>([0-9]\.[0-9])</average_rating>')
    __re_tv_test = re.compile('<category.*? label="Television" term="Television">')

    __xml_fields = { 'id' : 'id', 'title' : 'name', 'release_year' : 'year' }
    __call_limit = 4500

    def __init__(self):
        pass

    def produce_numbers(self):
        for title in self.__get_titles():
            yield title

    def fetch_data(self):
        self.__download_title_catalog()
        self.__download_titles()

    def __download_title_catalog(self):
        f = open(self.__titles_file, 'w')
        f.write(self.__fetch(self.__titles_url))
        f.close()

    def __download_titles(self):
        i = 0
        for title in self.__get_titles():
            title_key = str(title[1][1]['key'])
            title_path = self.__get_title_path(title_key)
            if not os.path.isfile(title_path):
                dirname = os.path.dirname(title_path)
                if not os.path.isdir(dirname):
                    os.makedirs(dirname)
                title_url = os.path.join(self.__title_url_base, title_key)
                f = open(title_path, 'w')

                contents = self.__fetch(title_url)
                if contents == '<h1>403 Developer Over Rate</h1>':
                    f.close()
                    raise Exception('Over Netflix API rate limit')
                f.write(contents)
                f.close()

                i += 1
                if i > self.__call_limit:
                    break

    def __get_titles(self):
        in_item = False
        skip_title = False
        h = HTMLParser.HTMLParser()
        for line in open(self.__titles_file):
            line_clean = line.strip() 
            if not in_item and line_clean == '<title_index_item>':
                in_item = True
                title = { 'type' : 'film' }

            if in_item and not skip_title:
                if self.__re_tv_test.match(line):
                    skip_title = True
                for field, title_key in self.__xml_fields.iteritems():
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

                title_key = self.__re_numeric_id.search(title['id']).group(1)
                del title['id']

                title_info = self.__get_title_info(title_key)
                if title_info:
                    title_rating = decimal.Decimal(title_info['rating'])
                else:
                    title_rating = 0
                numbers = { 'rating' : title_rating, 'key' : int(title_key) }
                yield [ title, [ 'netflix', numbers ] ]

    def __get_title_info(self, id):
        path = self.__get_title_path(id)
        if os.path.isfile(path):
            f = open(path, 'r')
            xml = f.read()
            f.close()
            rating_match = self.__re_title_rating.search(xml)
            rating = rating_match.group(1) if rating_match else 0
            return { 'rating' : rating }

    def __get_title_path(self, id):
        basename = '.'.join((id, 'xml'))
        bucket = id[:2]
        return os.path.join(self.__titles_folder, bucket, basename)

    def __fetch(self, url):
        consumer = oauth.Consumer(key=self.__consumer_key, secret=self.__consumer_secret)
        client = oauth.Client(consumer)

        resp, content = client.request(url, "GET")
        return content

if __name__ == '__main__':
    source = NetflixSource()
    source.download_titles()
