filmdata
========

Technically I use this to import lots of film data for my site, [filmlust](http://filmlust.com), but it's mostly just a playground for me. I see something that looks interesting and I bring it in here and build a prototype to experiment with it (mainly because that's how I learn). For instance, buried deep in the bowels of the code lies at least 3 implementations for scrapers (using tornado, gevent, and twisted. I think I'm going to settle on gevent, btw).

Sometimes things will reach a quiescent point and I clean it up a bit (e.g. the dynamic sqlalchemy models for automagically making tables for plugins). I guess what I'm saying is use at your own risk and feel free to contribute because there are probably many superior solutions out there and I'd like to see them.

Anyway, this thing will fetch raw data from the following sources:

* [imdb](http://www.imdb.com/interfaces)
* [netflix](http://developer.netflix.com/)
* [flixster/rotten tomatoes](http://developer.rottentomatoes.com/)

That data is then imported into one of the following sinks:

* [MongoDB](http://www.mongodb.org)

The following sinks are currently broken:

* [SQLAlchemy](http://www.sqlalchemy.org) (use your preferred relational db behind it)

Coming soon: freebase source? box office data? sqlalchemy working?

Usage
-----

Copy the `config_sample.ini` to `config.ini` and edit it to your liking.  There are also a couple more static options in the base `__init__.py` file.

This will show you all the options for fetching and importing:

    python main.py --help
