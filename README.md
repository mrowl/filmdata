filmdata
========

This library will fetch raw data from the following sources:
* [imdb](http://www.imdb.com/interfaces)
* [netflix](http://developer.netflix.com/)

That data is then imported into the following sinks:
* [SQLAlchemy](http://www.sqlalchemy.org) (use your preferred relational db behind it)

Coming soon: freebase source and mongodb sink

Usage
-----

Copy the `config_sample.ini` to `config.ini` and edit it to your liking

This will show you all the options for fetching and importing:

    python main.py --help
