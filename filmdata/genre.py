import re

GENRE_BIT_MAP = {
    'drama' : 0,
    'comedy' : 1,
    'short' : 2,
    'foreign' : 3,
    'horror' : 4,
    'documentary' : 5,
    'action' : 6,
    'adventure' : 7,
    'thriller' : 8,
    'romance' : 9,
    'crime' : 10,
    'family' : 11,
    'sci-fi' : 12,
    'fantasy' : 13,
    'mystery' : 14,
    'musical' : 15,
    'war' : 16,
    'western' : 17,
    'indie' : 18,
}

class Genres:
    _splitters = [
        re.compile('^(family) (.+?)$'),
        re.compile('^(foreign) (.+?)$'),
        re.compile('^(indie) (.+?)$'),
        re.compile('^(romantic) (.+?)$'),
        re.compile('^(classic) (.+?)$'),
        re.compile('^(crime) (dramas)$'),
        re.compile('^(sports) (dramas)$'),
        re.compile('^(social issue) (dramas)$'),
        re.compile('^(.+?) (period pieces)$'),
        re.compile('^(.+?) (thrillers)$'),
        re.compile('^(.+?) (classics)$'),
        re.compile('^(.+?) (documentaries)$'),
    ]

    _extractors = [
        re.compile('^(.+?) language$'),
        re.compile('^(sports) & fitness$'),
        re.compile('^action & (adventure)$'),
    ]

    _mappers = {
        'period piece' : 'period',
        'period pieces' : 'period',
        'dramas' : 'drama',
        'adventures' : 'adventure',
        'comedies' : 'comedy',
        'thrillers' : 'thriller',
        'music & musicals' : 'musical',
        'musicals' : 'musical',
        'children & family' : 'family',
        'romantic' : 'romance',
        'classics' : 'classic',
        'documentaries' : 'documentary',
        'historical' : 'history',
        'biographical' : 'biography',
        'sports' : 'sport',
        'westerns' : 'western',
    }

    _priority = [ k for k, v in sorted(GENRE_BIT_MAP.items(), key=lambda i: i[1]) ]

    def __init__(self, names):
        self.names = [ n.lower() for n in names ]
        self.labels = []
        self._build_list()

    def _build_list(self):
        split = self._run_splits(self.names)
        extracted = self._run_extractions(split)
        mapped = self._run_mappers(extracted)
        self.labels = self._order_set(mapped)

    def _order_set(self, names):
        ordered = [ p for p in self._priority if p in names ]
        return ordered + [ n for n in names if not n in ordered ]

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
