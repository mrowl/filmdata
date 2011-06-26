import logging
import time
import re
import cPickle
import os
from unicodedata import normalize

import Levenshtein

import filmdata.sink

log = logging.getLogger('filmdata.main')

TITLE_SCHEMA = {
    'rating' : 'append',
    'href' : 'append',
    'key' : 'append',
    'year' : ('netflix', 'imdb', 'flixster'),
    'name' : ('netflix', 'imdb', 'flixster'),
    'art' : ('netflix',),
    'type' : ('imdb', 'netflix', 'flixster'),
    'availability' : ('netflix',),
    'runtime' : ('imdb', 'flixster', 'netflix'),
    'award' : ('netflix', 'imdb'),
    'synopsis' : ('netflix', 'imdb'),
    'genre' : ('netflix', 'imdb', 'flixster'),
    'production' : ('imdb', 'netflix', 'flixster'),
    'cast' : ('imdb', 'netflix', 'flixster'),
    'aka' : ('imdb', 'netflix'),
}

def produce_merged_titles(major_name, minor_name):

    if os.path.exists('hort.cp'):
        print 'start loading from file'
        matches = cPickle.load(open('hort.cp'))
        print 'end loading from file'
    else:
        major = dict([ (t['key'], t) for t in 
                       filmdata.sink.get_titles_for_matching(major_name) ])
        minor = dict([ (t['key'], t) for t in
                       filmdata.sink.get_titles_for_matching(minor_name) ])
        matches = match_source_titles(major, minor)
        cPickle.dump(matches, open('hort.cp', 'w'))

    for minor_key, major_key in matches.iteritems():
        major_title = filmdata.sink.get_source_title_by_key(major_name,
                                                            major_key)
        minor_title = filmdata.sink.get_source_title_by_key(minor_name,
                                                            minor_key)
        if major_title is None: 
            print 'major uh oh'
        if minor_title is None:
            print 'minor uh oh'
        source_titles = {
            major_name : major_title,
            minor_name : minor_title,
        }
        new_title = merge_source_titles(**source_titles)
        yield new_title

def merge_source_titles(**source_titles):
    title = {}
    for title_key, merger in TITLE_SCHEMA.items():
        if merger == 'append':
            title[title_key] = dict([ (k, v[title_key]) for k, v in
                                      source_titles.items() if title_key in v])
        else:
            for source_name in merger:
                if (source_name in source_titles and
                    title_key in source_titles[source_name] and
                    source_titles[source_name][title_key]):
                    title[title_key] = source_titles[source_name][title_key]
                    break
            else:
                title[title_key] = None
    return title

def match_source_titles(major, minor):
    minor_to_major = {}
    major_to_minor = {}

    def cleanup():
        for k in major_to_minor.iterkeys():
            if k in major:
                del major[k]
        for k in minor_to_major.iterkeys():
            if k in minor:
                del minor[k]

    # lower is better
    def score_pair(one, two):
        score = 0

        if one['name'] != two['name']:
            one_names = set((one['name'], )) if not 'aka' in one else one['aka']
            two_names = set((two['name'], )) if not 'aka' in two else two['aka']
            if not len(one_names & two_names) > 0:
                pairs = [ (a, b) for a in one_names for b in two_names ] 
                for a, b in pairs:
                    if a in b or b in a:
                        score = 1
                        break
                else:
                    score += min([ Levenshtein.distance(n1, n2) for 
                                   n1 in one_names for n2 in two_names ])
        if one['year'] != two['year']:
            score += one['year'] - two['year']
        if 'director' in one and 'director' in two:
            one_len = len(one['director'])
            two_len = len(two['director'])
            if one_len == two_len:
                score += 2 * len(one['director'] - two['director'])
            elif one_len == 0 or two_len == 0:
                score += 2
            else:
                if one_len < two_len:
                    score += 2*len(one['director'] - two['director'])
                else:
                    score += 2*len(two['director'] - one['director'])
        return score

    def best_match(title, major_keys=None, minor_keys=None):
        if major_keys is not None:
            if len(major_keys) == 1:
                return major_keys[0]
            else:
                guesses = [ major[k] for k in major_keys ]
        elif minor_keys is not None:
            if len(minor_keys) == 1:
                return minor_keys[0]
            else:
                guesses = [ minor[k] for k in minor_keys ]
        else:
            return None

        best_yet = (9999, None)
        for guess in guesses:
            cur_score = score_pair(title, guess)
            if cur_score < best_yet[0]:
                best_yet = (cur_score, guess['key'])
        return best_yet[1]


    def index_matcher(key_gen):
        index = {}
        for k, t in major.iteritems():
            ikeys = key_gen(t)
            for ikey in ikeys:
                if ikey in index:
                    index[ikey].append(k)
                else:
                    index[ikey] = [ k, ]
                
        for i, title in enumerate(minor.itervalues()):
            ikeys = key_gen(title)
            for ikey in ikeys:
                if ikey in index:
                    major_key = best_match(title, index[ikey])
                    if major_key in major_to_minor:
                        # possible duplicates in the minor list
                        # see which of the these two keys is better
                        minor_key = best_match(major[major_key],
                                               minor_keys=[title['key'],
                                                           major_to_minor[major_key]])
                        chosen = 'current' if minor_key == title['key'] else 'prev'
                        log.debug("Duplicate in minor list? Both matching to same major title.")
                        log.debug("Selected %s (%s) in the end" % (str(minor_key), chosen))
                        log.debug("Second minor title (current one):")
                        log.debug(title)
                        log.debug("First minor title:")
                        log.debug(minor[major_to_minor[major_key]])
                        log.debug("The major title to which they are matching")
                        log.debug(major[major_key])
                    else:
                        minor_key = title['key']
                    minor_to_major[minor_key] = major_key
                    major_to_minor[major_key] = minor_key
        cleanup()

    def key_gen_aka(title):
        if 'aka' in title:
            years = set((title['year'], ))
            if 'aka_year' in title:
                years |= title['aka_year'] | set((title['year'] + 1,
                                                  title['year'] - 1))
            names = title['aka'] | set((title['name'], ))
            return [ (year, a) for year in years for a in names ]
        else:
            return ((title['year'], title['name']), )

    re_alpha = re.compile('[^A-Za-z0-9\_]')
    fuzz = lambda s: re_alpha.sub('', normalize('NFKD', s))
    key_gen_fuzzy_names = lambda t: [ (p[0], fuzz(p[1])) for
                                p in key_gen_aka(t) ]
    key_gen_fuzzy_dirs = lambda t: [ (t['year'], fuzz(d)) for
                                     d in t['director'] ] if 'director' in t else []

        #for i, minor_title in enumerate(minor):
    start = time.time()

    index_matcher(lambda t: ((t['name'], t['year']), ))
    index_matcher(key_gen_aka)
    index_matcher(key_gen_fuzzy_names)
    index_matcher(key_gen_fuzzy_dirs)
    #index_matcher(lambda t: t['name'])

    print len(minor_to_major)
    print time.time() - start
    return minor_to_major
