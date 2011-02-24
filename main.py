import logging
from optparse import OptionParser

from filmdata.sources.netflix import NetflixSource
from filmdata.sources.imdb import ImdbSource
#from filmdata.sinks.mongo import MongoSink

from filmdata import config

log = logging.getLogger('filmdata.main')

def run_roles_fetch(source, types):
    log.info('Fetching all active roles: %s' % str(types))
    source.fetch_roles(types)

def run_roles_import(source, sink, title_types, role_types):
    log.info('Importing all active roles: %s' % str(role_types))
    sink.consume_roles(source.produce_roles(title_types, role_types))

def run_aka_fetch(source):
    log.info('Fetching aka titles from source: %s' % source.name)
    source.fetch_aka_titles()

def run_aka_import(source, sink, types):
    log.info('Importing aka titles from source: %s' % source.name)
    sink.consume_aka_titles(source.produce_aka_titles(types))

def run_data_fetch(source):
    log.info('Fetching data from source: %s' % source.name)
    source.fetch_data()

def run_data_import(source, sink, types):
    log.info('Importing data from source: %s' % source.name)
    sink.consume_numbers(source.produce_numbers(types))

def main():
    parser = OptionParser()
    parser.add_option("-s", "--sqlalchemy", action="store_true",
                      dest="sa",
                      help="Use SQLAlchemy as the data sink (default)")
    #parser.add_option("-m", "--mongo", action="store_true",
                      #dest="mongo",
                      #help="Use MongoDB as the data sink [NOT FUNCTIONAL RIGHT NOW]")
    parser.add_option("--sink-init", action="store_true",
                      dest="sink_init",
                      help="Initialize your chosen sink (i.e. destroy data and build data(base|store) schema)")
    parser.add_option("--sink-install", action="store_true",
                      dest="sink_install",
                      help="Install your chosen sink (i.e. build data(base|store) schema)")
    #parser.add_option("--titles", action="append_const",
                      #dest="titles_update",
                      #help="Update the titles table (from IMDB)")
    #parser.add_option("--actors", action="append_const",
                      #dest="roles", const="actor"
                      #help="Update the actors table (from IMDB)")
    #parser.add_option("--actresses", action="append_const",
                      #dest="roles", const="actress"
                      #help="Update the actresses table (from IMDB)")
    #parser.add_option("--directors", action="append_const",
                      #dest="roles", const="actress"
                      #help="Update the directors table (from IMDB)")
    parser.add_option("--roles", action="store_true",
                      dest="roles_both",
                      help="Fetch and import all the active roles in the config (from IMDB)")
    parser.add_option("--roles-fetch", action="store_true",
                      dest="roles_fetch",
                      help="Fetch all the data for active roles in the config (from IMDB)")
    parser.add_option("--roles-import", action="store_true",
                      dest="roles_import",
                      help="Import all the data for active roles in the config (from IMDB)")
    parser.add_option("--aka", action="store_true",
                      dest="aka_both",
                      help="Fetch and import the aka titles data (from IMDB)")
    parser.add_option("--aka-fetch", action="store_true",
                      dest="aka_fetch",
                      help="Fetch the aka titles data (from IMDB)")
    parser.add_option("--aka-import", action="store_true",
                      dest="aka_import",
                      help="Import the aka titles data (from IMDB)")
    parser.add_option("-n", "--netflix", action="store_true",
                      dest="netflix_both",
                      help="Run both the netflix data fetch and import")
    parser.add_option("--netflix-fetch", action="store_true",
                      dest="netflix_fetch",
                      help="Run the netflix data fetch")
    parser.add_option("--netflix-import", action="store_true",
                      dest="netflix_import",
                      help="Run the netflix import")
    parser.add_option("-i", "--imdb", action="store_true",
                      dest="imdb_both",
                      help="Run both the imdb data fetch and import")
    parser.add_option("--imdb-fetch", action="store_true",
                      dest="imdb_fetch",
                      help="Run the imdb data fetch")
    parser.add_option("--imdb-import", action="store_true",
                      dest="imdb_import",
                      help="Run the imdb import")
    parser.add_option("--crunch", action="store_true",
                      dest="crunch",
                      help="Run the numbers")

    (options, args) = parser.parse_args()
    
    #if options.sa and options.mongo:
        #parser.error("options -s and -m are mutually exclusive.  only one sink allowed!")
    #elif options.mongo:
        #from filmdata.sinks.mongo.base import MongoSink as Sink
    #else:
        #log.info('Sink set to SQLAlchemy, all data will be directed there!')
        #from filmdata.sinks.sa.base import SaSink as Sink

    from filmdata.sinks.sa.base import SaSink as Sink
    log.info('Sink set to SQLAlchemy, all data will be directed there!')
    sink = Sink()
    if options.sink_init:
        sink.setup()
    elif options.sink_install:
        sink.install()

    active_role_types = config.get('core', 'active_role_types').split()
    active_title_types = config.get('core', 'active_title_types').split()

    if options.netflix_both:
        log.info('Running both the fetch and import for netflix...')
        run_data_fetch(NetflixSource())
        run_data_import(sink, NetflixSource(), active_title_types)
    elif options.netflix_fetch:
        run_data_fetch(NetflixSource())
    elif options.netflix_import:
        run_data_import(NetflixSource(), sink, active_title_types)

    if options.imdb_both:
        log.info('Running both the fetch and import for imdb...')
        run_data_fetch(ImdbSource())
        run_data_import(ImdbSource(), sink, active_title_types)
    elif options.imdb_fetch:
        run_data_fetch(ImdbSource())
    elif options.imdb_import:
        run_data_import(ImdbSource(), sink, active_title_types)

    if options.aka_both:
        log.info('Running both the fetch and import for aka titles...')
        run_aka_fetch(ImdbSource())
        run_aka_import(ImdbSource(), sink, active_title_types)
    elif options.aka_fetch:
        run_aka_fetch(ImdbSource())
    elif options.aka_import:
        run_aka_import(ImdbSource(), sink, active_title_types)

    if options.roles_both:
        log.info('Running both the fetch and import for roles...')
        run_roles_fetch(ImdbSource(), active_role_types)
        run_roles_import(ImdbSource(), sink, active_title_types,
                         active_role_types)
    elif options.roles_fetch:
        run_roles_fetch(ImdbSource(), active_role_types)
    elif options.roles_import:
        run_roles_import(ImdbSource(), sink, active_title_types,
                         active_role_types)

    if options.crunch:
        from filmdata.metric.title import Metric
        sink.consume_metrics(Metric().build(sink), 'metric_title')
        return 0
        for t in sink.get_persons_title_agg('director'):
            log.debug(str(t))

if __name__ == '__main__':
    main()
