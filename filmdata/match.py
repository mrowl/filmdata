import logging
import time
import re
import cPickle
import os
import hashlib
from unicodedata import normalize

import Levenshtein

import filmdata.sink
from filmdata.lib.util import base_encode
from filmdata.genre import Genres

log = logging.getLogger('filmdata.main')

TITLE_SCHEMA = {
    'rating' : 'append_plural',
    'href' : 'append',
    'key' : 'append',
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

MATCH_SCORE_THRESHOLD = .3

KEY_HASH_ORDER = ('imdb', 'netflix', 'flixster')

re_alpha = re.compile('[^A-Za-z0-9\_]')
fuzz = lambda s: re_alpha.sub('', normalize('NFD', s.lower()))

def keys_hash(keys):
    key_list = [ str(keys[k]) for k in KEY_HASH_ORDER if k in keys ]
    return base_encode(int(hashlib.md5(':'.join(key_list)).hexdigest()[:12],
                           16))

class Merge:

    def __init__(self):
        self._merged_titles = {}
        pass

    # takes in special producers used for title matching
    def produce_titles(self, *title_producers):

        #print Compare.title(thing_one, thing_two)
        #return
        #if os.path.exists('titles.cp'):
            #print 'start loading from file'
            #matches = cPickle.load(open('titles.cp'))
            #print 'end loading from file'
        #else:

        start = time.time()
        primary_name, primary_producer = title_producers[0]
        if os.path.exists('primary_titles.cp'):
            primary = cPickle.load(open('primary_titles.cp'))
        else:
            primary = dict([ (t['key'], t) for t in primary_producer ])
            cPickle.dump(primary, open('primary_titles.cp', 'w'))
        print '%f finished loading primary' % (time.time() - start)

        matches = dict([ (k, None) for k in primary.keys() ])
        print '%f finished making default matches' % (time.time() - start)

        for aux_name, aux_producer in title_producers[1:]:
            if os.path.exists('%s_titles.cp' % aux_name):
                aux = cPickle.load(open('%s_titles.cp' % aux_name))
            else:
                aux = dict([ (t['key'], t) for t in aux_producer ])
                cPickle.dump(aux, open('%s_titles.cp' % aux_name, 'w'))

            print '%f started aux matching: %s' % ((time.time() - start), aux_name)
            match = Match(primary.copy(), aux)
            aux_matches = match.titles()
            print '%d matches for %s' % (len(aux_matches), aux_name)
            print '%f finished aux matching: %s' % ((time.time() - start), aux_name)

            print '%f started aux key merging: %s' % ((time.time() - start), aux_name)
            for primary_key, aux_key in aux_matches.iteritems():
                if matches[primary_key] is None:
                    matches[primary_key] = { aux_name : aux_key }
                else:
                    matches[primary_key][aux_name] = aux_key
            print '%f finished aux key merging: %s' % ((time.time() - start), aux_name)
            #cPickle.dump(matches, open('titles.cp', 'w'))

        print '%f started old_title fetch' % (time.time() - start)
        old_titles = {}
        old_primaries = {}
        for old_title in filmdata.sink.get_titles():
            key_pairs = tuple([ (k, old_title['key'][k]) for k in
                                KEY_HASH_ORDER if
                                k in old_title['key'] and old_title['key'][k] ])
            old_titles[key_pairs] = old_title['id']
            old_primaries[old_title['key'][primary_name]] = old_title['id']
        print '%f finished old_title fetch' % (time.time() - start)

        print '%f started mass merging' % (time.time() - start)
        new_matches = 0
        for i, (primary_key, aux_keys) in enumerate(matches.iteritems()):
            source_keys = { primary_name : primary_key }
            aux_keys and source_keys.update(aux_keys)
            #source_key_pairs = tuple([ (k, source_keys[k]) for k in
                                       #KEY_HASH_ORDER if
                                       #k in source_keys ])
            source_titles = {}
            for source_name, source_key in source_keys.items():
                 source_titles[source_name] = filmdata.sink.get_source_title_by_key(
                     source_name, source_key)
            new_title = self._merge_source_titles(**source_titles)

            if primary_key in old_primaries:
                new_title['id'] = old_primaries[primary_key]
            else:
                new_matches += 1

            yield new_title
        print 'found %d new matches' % new_matches
        print '%f finished merging' % (time.time() - start)

    def produce_persons(self):
        main_source = 'imdb'
        aux_sources = ('netflix', 'flixster')
        persons = {}
        #for old_person in filmdata.sink.get_persons():
        for title in filmdata.sink.get_titles():
            members = title['cast'] if title.get('cast') else []
            for group in [ g for g in ('director', 'writer') if title.get(g) ]:
                for member in title[group]:
                    member['role'] = 'director'
                    members.append(member)

            for member in members:
                if not main_source in member['key']:
                    continue
                main_key = member['key'][main_source]
                if main_key in persons:
                    persons[main_key]['key'].update(member['key'])
                    persons[main_key]['href'].update(member['href'])
                else:
                    persons[main_key] = {
                        'key' : member['key'],
                        'href' : member['href'],
                        'name' : member['name'],
                    }

                role = member['role']
                role_entry = { 'title_id' : title['id'] }
                if member.get('billing'):
                    role_entry['billing'] = member['billing']
                if role == 'writer' or role == 'actor' or role == 'actress':
                    role_entry['role'] = member['role']
                if role == 'actor' or role == 'actress':
                    role_entry['character'] = member['character']

                if not persons[main_key][role]:
                    persons[main_key][role] = []
                persons[main_key][role].append(role_entry)

        return persons.itervalues()

    def _merge_source_titles(self, **source_titles):
        title = {}
        for title_key, merger in TITLE_SCHEMA.items():
            title[title_key] = None
            if merger in ('append', 'append_plural'):
                title[title_key] = dict([ (k, v[title_key]) for k, v in
                                          source_titles.items() if title_key in v])
                if merger == 'append_plural':
                    plural_key = title_key + 's'
                    for source_name, source_title in source_titles.items():
                        if source_title.get(plural_key):
                            for k, v in source_title[plural_key].items():
                                title[title_key][k] = v
            elif title_key in ('cast', 'writer', 'director'):
                args = dict([ (k, v[title_key]) for k, v in
                              source_titles.items() if
                              title_key in v and k in merger ])
                title[title_key] = self._merge_source_title_persons(merger, **args)
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
                        (not a.get('note') or not 
                         'working title' in a.get('note')) ]
            if us_name:
                return us_name[0]
        return None

    def _merge_source_title_persons(self, order, **source_persons):
        if not source_persons:
            return None
        persons = []
        if len(source_persons) < 2:
            source_name = source_persons.keys()[0]
            for person in source_persons.values()[0]:
                person['key'] = { source_name : person['key'] }
                if 'href' in person:
                    person['href'] = { source_name : person['href'] }
                persons.append(person)
            return persons

        source_names = [ k for k in order if k in source_persons.keys() ]
        mergee_source = source_names[0]
        mergees = source_persons[mergee_source]

        merged_count = 0
        for merger_source in source_names[1:]:
            mergers = source_persons[merger_source]
            for mergee in mergees: 
                mergee['key'] = { mergee_source : mergee['key'] }
                mergee['href'] = { mergee_source : mergee['href'] }
                for merger in mergers:
                    if mergee['name'] == merger['name']:
                        mergee['key'][merger_source] = merger['key']
                        break
                else:
                    mergee_fuzz = fuzz(mergee['name'])
                    for merger in mergers:
                        if mergee_fuzz == fuzz(merger['name']):
                            mergee['key'][merger_source] = merger['key']
                            break
                    else:
                        best_yet = (999, None)
                        for merger in mergers:
                            score = Levenshtein.distance(merger['name'],
                                                         mergee['name'])
                            if score < best_yet:
                                best_yet = (score, merger['key'])
                        if best_yet[0] < 4:
                            mergee['key'][merger_source] = best_yet[1]
                persons.append(mergee)
                if merger_source in mergee['key']:
                    merged_count += 1
                    if merged_count >= len(mergees):
                        break
        return persons

class Compare:

    @classmethod
    def title(cls, one, two):
        score = 0

        if fuzz(one['name']) != fuzz(two['name']):
            if 'aka' in one:
                one_names = frozenset([one['name'], ] + list(one['aka']))
            else:
                one_names = frozenset((one['name'], ))
            if 'aka' in two:
                two_names = frozenset([two['name'], ] + list(two['aka']))
            else:
                two_names = frozenset((two['name'], ))
            if not len(one_names & two_names) > 0:
                pairs = [ (a, b) for a in one_names for b in two_names ] 
                for a, b in pairs:
                    if a in b or b in a:
                        score += 1
                        break
                else:
                    score += 4 * min([ cls._lev(a, b) for a, b in pairs ])
        if one['year'] != two['year']:
            score += abs(one['year'] - two['year'])

        for set_key in ('director', 'cast'):
            if set_key == 'director':
                factor = 4
            else:
                factor = 3
            one_has = set_key in one and one[set_key]
            two_has = set_key in two and two[set_key]
            if one_has and two_has:
                score += factor * cls.name_sets(one[set_key], two[set_key])
            else:
                score += 2
        if score > 10:
            return float(1)
        return float(score) / 10

    @classmethod
    def name_sets(cls, one, two):
        assert len(one) > 0 and len(two) > 0
        # make alpha the larger set
        if len(one) >= len(two):
            alpha = one
            beta = two
        else:
            alpha = two
            beta = one
        if alpha == beta:
            return 0
        length_factor = 1 - (2 * float(len(beta)) / len(alpha))
        if beta <= alpha:
            return max(0, length_factor)
        scores = []
        for b in beta:
            scores.append(min([ cls._lev(a, b) for a in alpha ]))
        mean_score = float(sum(scores)) / len(scores)
        return mean_score * (float(len(beta - alpha)) / len(beta))

    @classmethod
    def _lev(cls, a, b):
        return float(Levenshtein.distance(a, b)) / max(len(a), len(b))


class Match:

    def __init__(self, major_set, minor_set):
        self._major = major_set
        self._minor = minor_set
        self._major_to_minor = {}
        self._minor_used = {}
        self._dupes = []

    def try_the_rest(self):
        for i, title in enumerate(self._minor.itervalues()):
            print i
            major_key, score = self.best_match(title, self._major)
            if score < MATCH_SCORE_THRESHOLD:
                print "\n"
                print title
                print self._major[major_key]

    def titles(self):
            #for i, minor_title in enumerate(minor):
        key_gen_fuzzy_names = lambda t: [ (p[0], fuzz(p[1])) for
                                    p in key_gen_aka(t) ]
        key_gen_fuzzy_dirs = lambda t: [ (t['year'], fuzz(d)) for
                                         d in t['director'] ] if 'director' in t else []

        key_gen_fuzzy_years = lambda t: [ (y, fuzz(t['name'])) for y in 
                                          range(t['year'] - 1, t['year'] + 2) ]

        def key_gen_aka(title):
            if 'aka' in title:
                years = set((title['year'], ))
                if 'aka_year' in title:
                    years |= title['aka_year'] | set((title['year'] + 1,
                                                      title['year'] - 1))
                names = title['aka'] | set((title['name'], ))
                return [ (y, n) for y in years for n in names ]
            else:
                return key_gen_fuzzy_years(title)

        start = time.time()

        print 'Doing name, year index pass'
        self._index_matcher(lambda t: ((t['name'], t['year']), ))
        print 'Doing fuzzy years index pass'
        self._index_matcher(key_gen_fuzzy_years)
        print 'Doing aka index pass'
        self._index_matcher(key_gen_aka)
        print 'Doing fuzzy names index pass'
        self._index_matcher(key_gen_fuzzy_names)
        print 'Doing fuzzy director names index pass'
        self._index_matcher(key_gen_fuzzy_dirs)
        print 'Doing the rest (finding best of remaining titles)'
        self.report()
        #try_the_rest()
        #index_matcher(lambda t: t['name'])

        print time.time() - start
        return self._major_to_minor

    def report(self):
        popular_titles_missing = [ v for k, v in self._major.iteritems() if
                                   k not in self._major_to_minor and
                                   v['votes'] > 2000 ]
        f = open('missing.txt', 'a')
        for item in popular_titles_missing:
            f.write(str(item) + "\n")
        f.close()
        print "missing this many popular imdb titles: %d" % len(popular_titles_missing)

        print "This many dupes: %d" % len(self._dupes)

    def _cleanup(self):
        for k in self._major_to_minor.iterkeys():
            if k in self._major:
                del self._major[k]
        #for minor_key in self._major_to_minor.itervalues():
            #if minor_key in self._minor:
                #minor_used[minor_key] = self._minor[minor_key]
                #del self._minor[minor_key]

    # lower is better
    def _best_match(self, title, guesses):
        best_yet = (None, float(1))
        for guess in guesses:
            cur_score = Compare.title(title, guess)
            if cur_score <= best_yet[1]:
                best_yet = (guess['key'], cur_score)
        return best_yet

    def _index_matcher(self, key_gen):
        index = self._build_index(key_gen)
        for title in self._minor.itervalues():
            title_keys = key_gen(title)
            for tkey in title_keys:
                if tkey in index:
                    minor_key, major_key = self._get_index_match(title, index[tkey])
                    if minor_key and major_key:
                        self._major_to_minor[major_key] = minor_key
        self._cleanup()

    def _build_index(self, key_gen):
        index = {}
        for k, t in self._major.iteritems():
            ikeys = key_gen(t)
            for ikey in ikeys:
                if ikey in index:
                    index[ikey].append(k)
                else:
                    index[ikey] = [ k, ]
        return index

    def _get_index_match(self, title, index_keys):
        major_key, score = self._best_match(title, [ self._major[k] for k in index_keys ])
        if score > MATCH_SCORE_THRESHOLD:
            return None, None
        if major_key in self._major_to_minor:
            # possible duplicates in the minor list
            # see which of the these two keys is better
            prev_key = self._major_to_minor[major_key]
            if prev_key in self._minor:
                prev_title = self._minor[prev_key]
            else:
                prev_title = self._minor_used[prev_key]
            dupe_key, dupe_score = self._best_match(title, [prev_title])
            if (title['votes'] != prev_title['votes'] and
                self._best_match(title, [prev_title])[0] <= .2):
                if title['votes'] >= prev_title['votes']:
                    minor_key = title['key']
                    self._dupes.append(prev_title)
                else:
                    minor_key = prev_key
                    self._dupes.append(title)
            else:
                minor_key, s = self._best_match(self._major[major_key], [title, prev_title])
            chosen = 'current' if minor_key == title['key'] else 'prev'
            log.debug("Duplicate in minor list? Both matching to same major title.")
            log.debug("Selected %s (%s) in the end" % (str(minor_key), chosen))
            log.debug("Second minor title (current one):")
            log.debug(title)
            log.debug("First minor title:")
            log.debug(self._minor[prev_key] if prev_key in self._minor else self._minor_used[prev_key])
            log.debug("The major title to which they are matching")
            log.debug(self._major[major_key])
        else:
            minor_key = title['key']
        return minor_key, major_key
