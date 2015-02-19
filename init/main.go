// Copyright (c) 2015 Niklas Wolber
// This file is licensed under the MIT license.
// See the LICENSE file for more information.
package main

import (
	"flag"
	"fmt"
	"log"

	r "github.com/dancannon/gorethink"
)

func main() {
	var fromMySQL, help bool
	flag.BoolVar(&fromMySQL, "fromMySQL", false, "Whether data from the MySQL database should be transformed into the new tables")
	flag.BoolVar(&help, "help", false, "Display this help")
	flag.Parse()

	if help {
		fmt.Println("go run main.go [-fromMySQL]")
		flag.PrintDefaults()
		return
	}

	session, err := r.Connect(r.ConnectOpts{
		Address: "localhost:28015",
	})

	log.Println("Create database 'angles'.")
	if _, err := r.DbCreate("angles").Run(session); err != nil {
		log.Fatalln(err)
	}
	session.Use("angles")

	if err != nil {
		log.Fatalln(err)
	}

	// Create table tweetedUris and indices
	log.Println("Creating table 'tweetedUris'.")
	if _, err = r.Db("angles").TableCreate("tweetedUris").Run(session); err != nil {
		log.Fatalln(err)
	}

	log.Println("Creating index 'tweetedUris.uri'.")
	if _, err = r.Table("tweetedUris").IndexCreate("uri").Run(session); err != nil {
		log.Fatalln(err)
	}

	log.Println("Creating index 'tweetedUris.explorer'.")
	if _, err = r.Table("tweetedUris").IndexCreate("explorer").Run(session); err != nil {
		log.Fatalln(err)
	}

	// create a table to hold explorers
	log.Println("Creating table 'explorers'.")
	if _, err := r.Db("angles").TableCreate("explorers").Run(session); err != nil {
		log.Fatalln(err)
	}
	// create index in order to speed up later orderBy
	log.Println("Creating index 'explorers.fetchTime'.")
	if _, err := r.Table("explorers").IndexCreate("fetchTime").Run(session); err != nil {
		log.Fatalln(err)
	}

	if fromMySQL {
		log.Println("Importing explorers from MySQL data.")
		// construct a sequence of JSON objects, which look like this:
		// {
		//     screen_name: "xyz",
		//     id: 123,
		//     fetchTime: 0,
		//     maxID: 0,
		// }
		users := r.
			Table("tweets").
			Field("user").
			WithFields("id", "screen_name").
			Merge(r.Object("maxID", 0, "fetchTime", 0))

		result, err := r.
			Table("explorers").
			Insert(users).
			RunWrite(session)

		if err != nil {
			log.Fatalln(err)
		}

		if result.Errors > 0 {
			log.Println(result.FirstError)
		}

		log.Println("Deleting tweets without URIs from MySQL data.")
		// Remove tweets, without URLs
		urlEmpty := r.Row.Field("entities").Field("urls").IsEmpty()
		result, err = r.
			Table("tweets").
			Filter(urlEmpty).
			Delete().
			RunWrite(session)

		if err != nil {
			log.Fatalln(err)
		}

		if result.Errors > 0 {
			log.Println(result.FirstError)
		}

		log.Println("Importing URIs from MySQL data.")
		// selects the expanded_url property from the following JSON:
		// {
		// 	"display_url": "kritikundpraxis.org",
		// 	"expanded_url": http: //kritikundpraxis.org,
		// 	"indices": [0,22],
		// 	"url": http: //t.co/CFhZ3zpvU2,
		// }
		selectExpandedURLs := func(row r.Term) interface{} {
			return row.Field("expanded_url")
		}
		// merges an URI with the explorers id, who tweeted the uri
		selectUserURIPairs := func(tweet r.Term) interface{} {
			return tweet.Field("entities").Field("urls").Map(selectExpandedURLs).ForEach(func(url r.Term) interface{} {
				return r.Object("explorer", tweet.Field("user").Field("id")).Merge(r.Object("uri", url))
			})
		}
		userURIPairs := r.
			Table("tweets").
			Map(selectUserURIPairs)

		_, err = r.
			Table("tweetedUris").
			Insert(userURIPairs).
			Run(session)

		if err != nil {
			log.Fatalln(err)
		}
	}

}
