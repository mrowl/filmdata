import logging
import time
import re
from itertools import imap, ifilter
from functools import partial
from operator import itemgetter
from unicodedata import normalize

import Levenshtein

import filmdata.sink
from filmdata import config

log = logging.getLogger('filmdata.main')

MATCH_SCORE_THRESHOLD = .3

re_alpha = re.compile('[^A-Za-z0-9\_]')
fuzz = lambda s: re_alpha.sub('', normalize('NFD', s.lower()))
match_iter = lambda m: [ (k, v) for k, v in m.items() if k != 'id' and v ]

class Match:

    def __init__(self, type='title'):
        self._type = type
        self._primary_name = config.core.primary_title_source
        self._aux_names = [ n for n in config.sources if
                            n != self._primary_name ]

        self._females = {}
        self._matches_og = {}
        self._matches_source = { self._primary_name : {} }
        self._new_matches = {}
        self._indexes = {}

    def produce(self, status=None):
        if self._type == 'title':
            return self.produce_titles(status)
        elif self._type == 'person':
            return self.produce_persons(status)

    def produce_titles(self, title_status=None):
        self._build_og()
        self._build_primary()
        for source in self._aux_names:
            for title in self._get_things(source, title_status):
                match_ids = self._find_matches(title)
                if not match_ids:
                    self._mark_unmatched(source, title['id'])
                moved = self._get_moved_matches(title['id'], match_ids, source)
                new = self._get_new_matches(title['id'], match_ids, source)
                for match in new + moved:
                    #this is now an "original match" since it's in the db
                    yield self._update_og_match(match)

    def produce_persons(self, person_status=None):
        self._build_og()
        self._build_primary()
        if 1 == 0:
            yield { 'hort' : 'who' }

    def _get_things(self, source, status):
        if self._type == 'title':
            return filmdata.sink.get_titles_for_matching(source, status)
        if self._type == 'person':
            return filmdata.sink.get_persons_for_matching(source, status)

    def _get_thing(self, source, id):
        if self._type == 'title':
            return filmdata.sink.get_title_for_matching(source, id)
        if self._type == 'person':
            return filmdata.sink.get_person_for_matching(source, id)

    def _build_og(self):
        map(self._update_og_match,
            filmdata.sink.get_matches(type=self._type, status=('all', )))

    def _update_og_match(self, match_in):
        # check for sources that used to be in this match
        match = self._update_og_removals(match_in)
        self._matches_og[match['id']] = match.copy()
        for k, v in match_iter(match):
            if not k in self._matches_source:
                self._matches_source[k] = {}
            if v in self._matches_source[k]:
                #print '%s: two match_ids for same source_id' % k
                self._matches_source[k][v].add(match['id'])
            else:
                self._matches_source[k][v] = set((match['id'], ))
        return match

    def _update_og_removals(self, match):
        if not match['id'] in self._matches_og:
            return match
        new_match = match.copy()
        for prev_name, prev_id in match_iter(self._matches_og[match['id']]):
            if not prev_name in match:
                # remove this from the match_sources index
                self._matches_source[prev_name][prev_id].remove(match['id'])

            if match.get(prev_name, prev_id) != prev_id:
                # two source_ids for the same match_id, not allowed
                new_id = match[prev_name]
                prev_thing = self._get_thing(prev_name, prev_id)
                new_thing = self._get_thing(prev_name, new_id)
                match_thing = self._females[match['id']]
                best_id, score = self._best_match(match_thing,
                                                  [prev_thing, new_thing])
                # new thing wins!
                if best_id == new_id:
                    # remove the match for the prev thing
                    self._matches_source[prev_name][prev_id].remove(match['id'])
                else:
                    # reset the match to it's old state
                    new_match[prev_name] = prev_id

            # check if we've removed all of the matches for a source thing
            if not self._matches_source[prev_name][prev_id]:
                self._mark_unmatched(prev_name, prev_id)
                del self._matches_source[prev_name][prev_id]
        return new_match

    def _mark_unmatched(self, source, id):
        filmdata.sink.mark_source_title_unmatched(source, id)

    def _build_primary(self):
        for thing in self._get_things(self._primary_name, status=('all', )):
            if not thing['id'] in self._matches_source[self._primary_name]:
                #default all matches to the primary source
                #get a new id for this new title from the primary
                match = { self._primary_name : thing['id'] }
                match_id = filmdata.sink.consume_match(match, self._type)
                match['id'] = match_id
                self._update_og_match(match)
            elif len(self._matches_source[self._primary_name][thing['id']]) > 1:
                raise Exception('Primary id maps to multiple match ids')
            else:
                match_id = list(
                    self._matches_source[self._primary_name][thing['id']])[0]

            #TODO: make a title merger here for the matcher
            merged_thing = thing.copy()
            merged_thing['id'] = match_id
            self._females[match_id] = merged_thing

    def _get_moved_matches(self, title_id, match_ids, source):
        if (not source in self._matches_source or
            not self._matches_source[source].get(title_id)):
            return []

        #only need to remove the ids that were in the prev
        #set and not in the new one
        prev_match_ids = self._matches_source[source][title_id]
        moved_ids = prev_match_ids - match_ids
        if not moved_ids:
            return []

        #remove the source : title_id from any old matches
        remover = lambda p: dict([ (k, v) for k, v in 
                                   self._matches_og[p].items() if
                                   k != source or v != title_id ])
        return map(remover, moved_ids)

    def _get_new_matches(self, title_id, match_ids, source):
        #add new matches if they exist
        def adder(match_id):
            new_match = self._matches_og[match_id].copy()
            new_match[source] = title_id
            return new_match

        return map(adder, match_ids)

    def _find_matches(self, title):
        def key_gen_fuzzy_names(title):
            return [ (p[0], fuzz(p[1])) for p in key_gen_aka(title) ]

        def key_gen_fuzzy_dirs(title):
            return [ (title['year'], fuzz(d)) for
                     d in title['director'] ] if 'director' in title else []
        def key_gen_fuzzy_years(title):
            return [ (y, fuzz(title['name'])) for y in 
                     range(title['year'] - 1, title['year'] + 2) ]
        def key_gen_name_year (title):
            return ((title['name'], title['year']), )

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

        matches = set()
        matches |= self._alternate_matcher(title)
        matches |= self._index_matcher(title, key_gen_name_year)
        matches |= self._index_matcher(title, key_gen_fuzzy_years)
        matches |= self._index_matcher(title, key_gen_aka)
        matches |= self._index_matcher(title, key_gen_fuzzy_names)
        matches |= self._index_matcher(title, key_gen_fuzzy_dirs)
        return matches

    def _guess_id_score(self, title, guess):
        return guess['id'], Compare.title(title, guess)

    def _alternate_matcher(self, title):
        match_ids = set()
        if not title.get('alternate'):
            return match_ids
        for name, id in title['alternate'].items():
            match_ids |= self._matches_source[name].get(id, set())
        return match_ids

    def _index_matcher(self, title, key_gen):
        index = self._build_index(key_gen)
        title_keys = key_gen(title)
        matches = set()
        for tkey in title_keys:
            if tkey in index:
                matches |= set(
                    map(itemgetter(0),
                        filter(lambda m: m[1] <= MATCH_SCORE_THRESHOLD,
                            map(partial(self._guess_id_score, title),
                                [ self._females[k] for k in index[tkey] ]))))
        return matches

    def _build_index(self, key_gen):
        index_name = key_gen.__name__
        if not index_name in self._indexes:
            print 'Building index: %s' % key_gen.__name__
            index = {}
            for k, t in self._females.iteritems():
                ikeys = key_gen(t)
                for ikey in ikeys:
                    if not ikey in index:
                        index[ikey] = set()
                    index[ikey].add(k)
            self._indexes[index_name] = index
        return self._indexes[index_name]

    #def try_the_rest(self):
        #for i, title in enumerate(self._minor.itervalues()):
            #print i
            #major_key, score = self.best_match(title, self._major)
            #if score < MATCH_SCORE_THRESHOLD:
                #print "\n"
                #print title
                #print self._major[major_key]

    # lower is better
    def _best_match(self, title, guesses):
        best_yet = (None, float(1))
        for guess in guesses:
            cur_score = Compare.title(title, guess)
            if cur_score <= best_yet[1]:
                best_yet = (guess['id'], cur_score)
        return best_yet

    #def _get_index_match(self, title, index_keys):
        #match_id, score = self._best_match(title,
                                           #[ self._females[k] for k
                                             #in index_keys ])
        #if score > MATCH_SCORE_THRESHOLD:
            #return None
        #return match_id
        #if major_key in self._major_to_minor:
            ## possible duplicates in the minor list
            ## see which of the these two keys is better
            #prev_key = self._major_to_minor[major_key]
            #if prev_key in self._minor:
                #prev_title = self._minor[prev_key]
            #else:
                #prev_title = self._minor_used[prev_key]
            #dupe_key, dupe_score = self._best_match(title, [prev_title])
            #if (title['votes'] != prev_title['votes'] and
                #self._best_match(title, [prev_title])[0] <= .2):
                #if title['votes'] >= prev_title['votes']:
                    #minor_key = title['id']
                    #self._dupes.append(prev_title)
                #else:
                    #minor_key = prev_key
                    #self._dupes.append(title)
            #else:
                #minor_key, s = self._best_match(self._major[major_key], [title, prev_title])
            #chosen = 'current' if minor_key == title['id'] else 'prev'
            #log.debug("Duplicate in minor list? Both matching to same major title.")
            #log.debug("Selected %s (%s) in the end" % (str(minor_key), chosen))
            #log.debug("Second minor title (current one):")
            #log.debug(title)
            #log.debug("First minor title:")
            #log.debug(self._minor[prev_key] if prev_key in self._minor else self._minor_used[prev_key])
            #log.debug("The major title to which they are matching")
            #log.debug(self._major[major_key])
        #else:
            #minor_key = title['id']
        #return minor_key, major_key

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
