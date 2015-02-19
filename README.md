go-angles is a Go toolchain for the [Angles project](https://github.com/sscdotopen/angles). It is specifically tailored for retrieving the URIs tweeted.

#Install
```sh
go get -u github.com/nwolber/go-angles
```
You will also need [RethinkDB](http://rethinkdb.com/). Installation instructions can be found [here](http://rethinkdb.com/docs/install/). The database servers web GUI is available on http://localhost:8080.
##Migrate MySQL data
Export the data from MySQL:
```sql
SELECT json FROM tweets INTO OUTFILE /tmp/tweets.json;
```
Process file:
```sh
sed 's/\\\\/\\/g' tweets.json > tweets_fixed.json
```
Import into RethinkDB:
```sh
rethinkdb import -f tweets_fixed.json --table angles.tweets
```

#Init
Init initializes the database and sets up necessary tables.
```sh
go run init/main.go
```
#Timelines
Timelines fetches the Twitter timelines of the users present in the explorers table.
```sh
go run timelines/main.go
```
#Crawl
Crawl resolves URIs present in tweets to there "real" URIs (i.e. follows redirects).
```sh
go run crawl/main.go
```
#License
See LICENSE file.
