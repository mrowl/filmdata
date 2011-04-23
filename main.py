#!/usr/bin/python
import logging
from optparse import OptionParser

import filmdata
from filmdata import config
from filmdata.metric import manager as metric_manager

log = logging.getLogger('filmdata.main')

def run_roles_fetch(source, types):
    log.info('Fetching all active roles: %s' % str(types))
    source.fetch_roles(types)

def run_roles_import(source, title_types, role_types):
    log.info('Importing all active roles: %s' % str(role_types))
    filmdata.sink.consume_roles(source.produce_roles(title_types, role_types))

def run_aka_fetch(source):
    log.info('Fetching aka titles from source: %s' % source.name)
    source.fetch_aka_titles()

def run_aka_import(source, types):
    log.info('Importing aka titles from source: %s' % source.name)
    filmdata.sink.consume_aka_titles(source.produce_aka_titles(types))

def run_data_fetch(source):
    log.info('Fetching data from source: %s' % source.name)
    source.fetch_data()

def run_data_import(source, types):
    log.info('Importing data from source: %s' % source.name)
    filmdata.sink.consume_data(source.produce_data(types))

def crunch(option, opt_str, value, parser):
    if value and value != 'all':
        names = value.split(',')
        for name in names:
            metric = metric_manager.load(name)
            metric.run(filmdata.sink)
    else:
        for name, metric in metric_manager.iter():
            metric.run(filmdata.sink)

def main():
    if config.get('core', 'active_sink') == 'sqlalchemy':
        from filmdata.sinks.sa.base import SaSink as Sink
        log.info('Sink set to SQLAlchemy, all data will be directed there!')
        filmdata.sink = Sink()

    master_source_name = config.get('core', 'master_source')

    parser = OptionParser()
    parser.add_option("--sink-init", action="store_true",
                      dest="sink_init",
                      help="Initialize your chosen sink (i.e. destroy data and build data(base|store) schema)")
    parser.add_option("--sink-install", action="store_true",
                      dest="sink_install",
                      help="Install your chosen sink (i.e. build data(base|store) schema)")
    parser.add_option("--roles", action="store_true",
                      dest="roles_both",
                      help="Fetch and import all the active roles in the config (from %s)" % master_source_name)
    parser.add_option("--roles-fetch", action="store_true",
                      dest="roles_fetch",
                      help="Fetch all the data for active roles in the config (from %s)" % master_source_name)
    parser.add_option("--roles-import", action="store_true",
                      dest="roles_import",
                      help="Import all the data for active roles in the config (from %s)" % master_source_name)
    parser.add_option("--aka", action="store_true",
                      dest="aka_both",
                      help="Fetch and import the aka titles data (from %s) % master_source_name")
    parser.add_option("--aka-fetch", action="store_true",
                      dest="aka_fetch",
                      help="Fetch the aka titles data (from %s)" % master_source_name)
    parser.add_option("--aka-import", action="store_true",
                      dest="aka_import",
                      help="Import the aka titles data (from %s)" % master_source_name)
    parser.add_option("-c", "--crunch", action="callback",
                      callback=crunch, type="string",
                      help="Run the numbers")
    parser.add_option("-f", "--fetch", action="store", dest="fetches",
                      help="Run data fetch for source(s) (comma separated)")
    parser.add_option("-i", "--import", action="store", dest="imports",
                      help="Run data import for source(s) (comma separated)")

    (options, args) = parser.parse_args()
    
    if options.sink_init:
        filmdata.sink.setup()
    elif options.sink_install:
        filmdata.sink.install()

    active_role_types = config.get('core', 'active_role_types').split()
    active_title_types = config.get('core', 'active_title_types').split()

    if options.fetches:
        for name in options.fetches.split(','):
            source = filmdata.source.manager.load(name)
            run_data_fetch(source.Fetch)

    if options.imports:
        for name in options.imports.split(','):
            source = filmdata.source.manager.load(name)
            run_data_import(source.Produce, active_title_types)

    master_source = filmdata.source.manager.load(master_source_name)

    if options.aka_both:
        log.info('Running both the fetch and import for aka titles...')
        run_aka_fetch(master_source.Fetch)
        run_aka_import(master_source.Produce, active_title_types)
    elif options.aka_fetch:
        run_aka_fetch(master_source.Fetch)
    elif options.aka_import:
        run_aka_import(master_source.Produce, active_title_types)

    if options.roles_both:
        log.info('Running both the fetch and import for roles...')
        run_roles_fetch(master_source.Fetch, active_role_types)
        run_roles_import(master_source.Produce, active_title_types,
                         active_role_types)
    elif options.roles_fetch:
        run_roles_fetch(master_source.Fetch, active_role_types)
    elif options.roles_import:
        run_roles_import(master_source.Produce, active_title_types,
                         active_role_types)

if __name__ == '__main__':
    main()
