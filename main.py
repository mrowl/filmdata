#!/usr/bin/env python
import logging
from optparse import OptionParser

import filmdata
import filmdata.source
import filmdata.match
import filmdata.merge
from filmdata import config
from filmdata.metric import manager as metric_manager

log = logging.getLogger('filmdata.main')

def run_roles_fetch(source, types):
    log.info('Fetching all active roles: %s' % str(types))
    source.fetch_roles(types)

def run_roles_import(source, title_types, role_types):
    log.info('Importing all active roles: %s' % str(role_types))
    filmdata.sink.consume_roles(source.produce_roles(title_types, role_types),
                                source.name)

def run_aka_fetch(source):
    log.info('Fetching aka titles from source: %s' % source.name)
    source.fetch_aka_titles()

def run_aka_import(source, types):
    log.info('Importing aka titles from source: %s' % source.name)
    filmdata.sink.consume_title_akas(source.produce_title_akas(types), source.name)

def run_data_fetch(source):
    log.info('Fetching data from source: %s' % source.name)
    source.fetch_data()

def run_data_import(source, types):
    log.info('Importing data from source: %s' % source.name)
    filmdata.sink.consume_source_titles(source.produce_titles(types), source.name)

def crunch(option, opt_str, value, parser):
    if value and value != 'all':
        names = value.split(',')
        for name in names:
            metric = metric_manager.load(name)()
            filmdata.sink.consume_metric(name, metric())
    else:
        for name, metric_class in metric_manager.iter():
            metric = metric_class()
            filmdata.sink.consume_metric(name, metric())

def main():
    if config.core.active_sink == 'sqlalchemy':
        from filmdata.sink.sa.base import SaSink as Sink
        log.info('Sink set to SQLAlchemy, all data will be directed there!')
        filmdata.sink = Sink()
    elif config.core.active_sink == 'mongo':
        from filmdata.sink.mongo import MongoSink as Sink
        log.info('Sink set to MongoDB, all data will be directed there!')
        filmdata.sink = Sink()

    master_source_name = config.core.master_source

    parser = OptionParser()
    parser.add_option("--sink-init", action="store_true",
                      dest="sink_init",
                      help="""Initialize your chosen sink (i.e. destroy data
                              and build data(base|store) schema)""")
    parser.add_option("--sink-install", action="store_true",
                      dest="sink_install",
                      help="""Install your chosen sink
                      (i.e. build data(base|store) schema)""")
    parser.add_option("-c", "--crunch", action="callback",
                      callback=crunch, type="string",
                      help="Run the numbers")

    parser.add_option("-t", "--title", action="store_true", dest="op_title",
                      help="Limit the import to just titles")
    parser.add_option("-p", "--person", action="store_true", dest="op_person",
                      help="Limit the import to just persons")
    parser.add_option("-f", "--fetch", action="store", dest="fetches",
                      help="Run data fetch for source(s) (comma separated)")
    parser.add_option("-i", "--import", action="store", dest="imports",
                      help="Run data import for source(s) (comma separated)")
    parser.add_option("-v", "--votes", action="store_true", dest="op_votes",
                      help="Limit the fetch to votes for specified source")
    parser.add_option("-d", "--ids", action="store", dest="op_ids",
                      help="Limit the fetch to ids for specified source")
    parser.add_option("--match", action="store_true", dest="match",
                      help="match all source things into tuples of their ids")
    parser.add_option("--merge", action="store_true", dest="merge",
                      help="merge all the matched id tuples")
    parser.add_option("--all", action="store_true", dest="all",
                      help="operate on all items instead of just diff ones")
    parser.add_option("--nflx", action="store_true", dest="nflx_test",
                      help="operate on all items instead of just diff ones")
    (options, args) = parser.parse_args()
    
    if options.sink_init:
        filmdata.sink.setup()
    elif options.sink_install:
        filmdata.sink.install()

    active_title_types = config.core.active_title_types.split()
    active_role_types = config.core.active_role_types.split()

    if options.all:
        status = ('all',)
    else:
        status = ('new', 'updated', None)

    if options.nflx_test:
        source = filmdata.source.manager.load('netflix')
        res = source.Fetch._fetch('http://api.netflix.com/catalog/titles?term=the%20beaver')
        print res

    if options.match:
        if options.op_title:
            match = filmdata.match.Match('title')
            filmdata.sink.consume_matches(match.produce(status=status), 'title')
        elif options.op_person:
            match = filmdata.match.Match('person')
            filmdata.sink.consume_matches(match.produce(status=status), 'person')

    if options.merge:
        if options.op_title:
            merge = filmdata.merge.Merge('title')
            filmdata.sink.consume_merged_titles(
                merge.produce(match_status=status))
            log.info('Done merging titles')
        elif options.op_person:
            merge = filmdata.merge.Merge('person')
            filmdata.sink.consume_merged_persons(
                merge.produce(match_status=status))
            log.info('Done merging persons')
        log.info('Started crunching titles')
        crunch(None, 'title', None, None)
        log.info('Finished crunching titles')
        log.info('Started crunching persons')
        crunch(None, 'person', None, None)
        log.info('Finished crunching persons')

    if options.fetches:
        for name in options.fetches.split(','):
            source = filmdata.source.manager.load(name)
            if options.op_votes:
                source.Fetch.fetch_votes(fetch_existing=options.all)
            elif options.op_ids:
                source.Fetch.fetch_ids(active_title_types, options.op_ids)
            else:
                run_data_fetch(source.Fetch)

    if options.imports:
        for name in options.imports.split(','):
            source = filmdata.source.manager.load(name)
            if options.op_title:
                filmdata.sink.consume_source_titles(
                    source.Produce.produce_titles(active_title_types),
                    source.Produce.name)
            elif options.op_person:
                for role_type in active_role_types:
                    filmdata.sink.consume_source_persons(
                        source.Produce.produce_persons(role_type,
                                                       sans_roles=True),
                        source.Produce.name)

if __name__ == '__main__':
    main()
