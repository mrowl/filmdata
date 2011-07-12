import logging
import time
from operator import itemgetter

import filmdata.sink
from filmdata import config
from filmdata.genre import Genres

log = logging.getLogger('filmdata.main')

KEY_HASH_ORDER = ('imdb', 'netflix', 'flixster')

TITLE_SCHEMA = {
    'rating' : 'append_plural',
    'href' : 'append',
    'alternate' : 'append_from',
    'mpaa' : ('imdb', 'netflix', 'flixster'),
    'year' : ('netflix', 'imdb', 'flixster'),
    'name' : ('imdb', 'netflix', 'flixster'),
    'art' : ('netflix',),
    'type' : ('imdb', 'netflix', 'flixster'),
    'availability' : ('netflix',),
    'runtime' : ('imdb', 'netflix', 'flixster'),
    'award' : ('netflix', 'imdb'),
    'synopsis' : ('netflix', 'imdb'),
    'genre' : ('netflix', 'imdb', 'flixster'),
    'director' : ('imdb', 'netflix'),
    'writer' : ('imdb', 'netflix'),
    'cast' : ('imdb', 'netflix'),
    'aka' : ('imdb', 'netflix'),
}

PERSON_SCHEMA = {
    'key' : 'append',
    'href' : 'append',
    'name' : ('imdb', 'netflix', 'flixster'),
}

class Merge:

    def __init__(self):
        self._primary_person_source = config.core.primary_person_source
        self._primary_title_source = config.core.primary_title_source

    @property
    def person_ids(self):
        if not hasattr(self, '_person_ids'):
            self._person_ids = dict([ (p['alternate'][self._primary_person_source], p['id'])
                                       for p in filmdata.sink.get_persons() ])
        return self._person_ids

    def titles(self):
        start = time.time()
        for match in filmdata.sink.get_matches('title'):
            sources = dict([ (n, filmdata.sink.get_source_title_by_id(n, i)) for
                             n, i in match.items() if n != 'id' ])
            yield self._merge_source_titles(match['id'], **sources)
        print '%f finished merging' % (time.time() - start)

    def produce_persons(self, primary_source, aux_sources=None):
        primary_name, primary_persons = primary_source
        old_primaries = {}

        start = time.time()
        for oldie in filmdata.sink.get_persons():
            old_primaries[oldie['alternate'][primary_name]] = oldie['id']
        print '%f finished old_person fetch' % (time.time() - start)

        for person in primary_persons:
            new_person = {
                'alternate' : { 
                    primary_name : person['id'],
                },
                'name' : person['name'],
                'href' : person['href'],
            }
            if person['id'] in old_primaries:
                new_person['id'] = old_primaries[person['id']]
            yield new_person

    def _merge_source_titles(self, id, **source_titles):
        title = { 'id' : id }
        for title_key, merger in TITLE_SCHEMA.items():
            title[title_key] = None
            if title_key == 'alternate':
                title[title_key] = dict([ (k, v['id']) for k, v in
                                          source_titles.items() ])
            elif merger in ('append', 'append_plural'):
                title[title_key] = dict([ (k, v[title_key]) for k, v in
                                          source_titles.items() if title_key in v])
                if merger == 'append_plural':
                    plural_key = title_key + 's'
                    for source_name, source_title in source_titles.items():
                        if source_title.get(plural_key):
                            for k, v in source_title[plural_key].items():
                                title[title_key][k] = v
            elif title_key in ('cast', 'writer', 'director'):
                title[title_key] = self._merge_source_title_persons(
                    source_titles['imdb'].get(title_key))
            elif title_key == 'genre':
                raw_genres = set()
                for source_name, source_title in source_titles.items():
                    if source_title.get(title_key):
                        raw_genres |= set(source_title[title_key])
                title[title_key] = Genres(raw_genres).labels
            else:
                source_fields = [ (s, source_titles[s][title_key]) for
                                  s in merger if s in source_titles and
                                  source_titles[s].get(title_key) ]
                if source_fields:
                    title[title_key] = source_fields.pop(0)[1]
                    if title_key == 'name' and source_fields:
                        for k, v in source_fields:
                            if not title.get('aka'):
                                title['aka'] = []
                            title['aka'].append({
                                'name' : v,
                                'region' : k,
                                'year' : source_titles[k]['year'],
                            })

        aka_name = self._pick_aka_name(title.get('aka'))
        if aka_name:
            title['aka'].append({
                'name' : title['name'],
                'region' : 'native',
                'year' : title['year'],
            })
            title['name'] = aka_name

        return title

    def _pick_aka_name(self, aka):
        if aka:
            us_name = [ a['name'] for a in aka if
                        a['region'] == 'USA' and
                        self._aka_note_filter(a.get('note')) ]
            if us_name:
                return us_name[0]
        return None
    
    def _aka_note_filter(self, note):
        if not note:
            return True
        else:
            return False
        notel = note.lower()
        bad_phrases = (
            'working title',
            'complete title',
            'informal title',
            'imax',
            'original script title',
        )
        for phrase in bad_phrases:
            if phrase in notel:
                return False
        return True

    def _merge_source_title_persons(self, persons):
        if not persons:
            return None
        merged_persons = []
        for person in persons:
            member = dict([ (k, v) for k, v in person.items() if
                            k not in ('person_id', 'name') ])
            member['name'] = person['name']
            if person.get('person_id') and self.person_ids.get(person['person_id']):
                member['person_id'] = self.person_ids[person['person_id']]
            merged_persons.append(member)
        return sorted(merged_persons, key=itemgetter('billing'))

