mosql-measurements
==================

Setup notes (for myself):

`bundle exec ruby measure-import.rb --mongo mongodb://localhost:10001 -n 200000 --sql postgres://postgres@localhost`

To generate graph files, switch `measurement` calls with `measurement_rubyprof` in `-import` and `-tail`.
