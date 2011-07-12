import re

from filmdata import config

class Genres:
    _splitters = [
        re.compile('^(family) (.+?)$'),
        re.compile('^(foreign) (.+?)$'),
        re.compile('^(indie) (.+?)$'),
        re.compile('^(romantic) (.+?)$'),
        re.compile('^(classic) (.+?)$'),
        re.compile('^(.+?) (dramas)$'),
        re.compile('^(dark humor) & (black comedies)$'),
        re.compile('^(.+?) (comedies)$'),
        re.compile('^(extreme combat) (mixed martial arts)$'),
        re.compile('^(.+?) based on (.+?)$'),
        re.compile('^(.+?) (period pieces)$'),
        re.compile('^(.+?) (thrillers)$'),
        re.compile('^(.+?) (classics)$'),
        re.compile('^(.+?) (documentaries)$'),
        re.compile('^(crime) (action)$'),
        re.compile('^(espionage) (action)$'),
        re.compile('^(military & war) (action)$'),
        re.compile('^(sci-fi) (adventure)$'),
        re.compile('^(sci-fi) (cult)$'),
        re.compile('^(sci-fi) (horror)$'),
        re.compile('^(action) (sci-fi & fantasy)$'),
        re.compile('^(alien) (sci-fi)$'),
    ]

    _extractors = [
        re.compile('^(.+?) language$'),
        re.compile('^(sports) & fitness$'),
        re.compile('^(sports) stories$'),
        re.compile('^(war) stories$'),
        re.compile('^(inspirational) stories$'),
        re.compile('^(silent) films$'),
        re.compile('^(heist) films$'),
        re.compile('^(heist) films$'),
        re.compile('^(animation) for grown-ups$'),
    ]

    _mappers = {
        'ages 5-7' : 'family',
        'ages 8-10' : 'family',
        'ages 11-12' : 'family',
        'children & family' : 'family',
        'kids & family' : 'family',
        'science fiction & fantasy' : 'sci-fi & fantasy',
        'science fiction' : 'sci-fi',
        'period piece' : 'period',
        'period pieces' : 'period',
        'spoofs and satire' : 'spoofs & satire',
        'spoofs' : 'spoof',
        'dramas' : 'drama',
        'epics' : 'epic',
        'adventures' : 'adventure',
        'comedies' : 'comedy',
        'black comedies' : 'black comedy',
        'thrillers' : 'thriller',
        'music & musicals' : 'musical',
        'musicals' : 'musical',
        'romantic' : 'romance',
        'classics' : 'classic',
        'classical instrumental music' : 'classical',
        'classical music' : 'classical',
        'documentaries' : 'documentary',
        'historical' : 'history',
        'biographical' : 'biography',
        'biographies' : 'biography',
        'sports' : 'sport',
        'westerns' : 'western',
        'film-noir' : 'film noir',
        'spain' : 'spanish',
        'france' : 'french',
        'italy' : 'italian',
        'germany' : 'german',
        'korea' : 'korean',
        'japan' : 'japanese',
        'russia' : 'russian',
    }

    _overriders = {
        'sci-fi & fantasy' : ('sci-fi', 'fantasy'),
        'action & adventure' : ('action', 'adventure'),
        'mystery & suspense' : ('mystery', 'suspense'),
        'suspense & thriller' : ('thriller', 'suspense'),
        'military & war' : ('military', 'war'),
    }

    _ignored = frozenset((
        'miscellaneous',
        'dark humor',
        'must-see',
        'the book',
    ))

    _priority = [ k for k, v in sorted(config.genre_to_bit_map.items(), key=lambda i: i[1]) ]

    def __init__(self, names):
        first_pass = self._build_list([ n.lower() for n in names ])
        second_pass = self._build_list(first_pass)
        third_pass = self._build_list(second_pass)
        self.labels = self._order_set(third_pass)

    def _build_list(self, names):
        split = self._run_splits(names)
        extracted = self._run_extractions(split)
        mapped = self._run_mappers(extracted)
        overridden = self._run_overriders(mapped)
        return set([ g for g in overridden if not g in self._ignored ])

    def _order_set(self, names):
        ordered = [ p for p in self._priority if p in names ]
        return ordered + [ n for n in names if not n in ordered ]
    
    def _run_overriders(self, names):
        buffer = set()
        for name in list(names):
            for over in self._overriders.get(name, []):
                if over in names:
                    break
            else:
                buffer.add(name)
        return buffer

    def _run_mappers(self, names):
        buffer = set()
        for name in names:
            if name in Genres._mappers:
                buffer.add(Genres._mappers[name])
            else:
                buffer.add(name)
        return buffer

    def _run_extractions(self, names):
        buffer = set()
        for name in names:
            for extractor in Genres._extractors:
                match = extractor.match(name)
                if match:
                    buffer.add(match.group(1))
                    break
            else:
                buffer.add(name)
        return buffer

    def _run_splits(self, names):
        buffer = set()
        for name in names:
            for splitter in Genres._splitters:
                match = splitter.match(name)
                if match:
                    buffer.add(match.group(1))
                    buffer.add(match.group(2))
                    break
            else:
                buffer.add(name)
        return buffer
