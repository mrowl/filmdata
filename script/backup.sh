#!/bin/bash
rm -rf data/backup
mkdir -p data/backup/imdb && cp data/sources/imdb/{person_ids,title_ids}.json data/backup/imdb/
mkdir -p data/backup/flixster && cp data/sources/flixster/{title_ids,titles}.json data/backup/flixster/
mkdir -p data/backup/netflix && cp data/sources/netflix/votes.json data/backup/netflix/

cd data/
tar -cjf data_bak.tar.bz2 backup
cp data_bak.tar.bz2 ~/Dropbox/filmdata/
cd ..
