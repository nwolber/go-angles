// Copyright (c) 2015 Niklas Wolber
// This file is licensed under the MIT license.
// See the LICENSE file for more information.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	twitter "github.com/ChimeraCoder/anaconda"
	r "github.com/dancannon/gorethink"
)

const (
	twitterConsumerKey       = "TWITTER_CONSUMER_KEY"
	twitterConsumerSecret    = "TWITTER_CONSUMER_SECRET"
	twitterApplicationKey    = "TWITTER_APPLICATION_KEY"
	twitterApplicationSecret = "TWITTER_APPLICATION_SECRET"
)

func main() {
	var help, timeline, timelineCsv, followers bool
	flag.BoolVar(&help, "h", false, "Display this help")
	flag.BoolVar(&timeline, "t", false, "Fetch user timelines")
	flag.BoolVar(&timelineCsv, "tcsv", false, "Export tweeted uris to csv")
	flag.BoolVar(&followers, "f", false, "Fetch followers")
	flag.Parse()

	if help {
		printHelp()
		return
	}

	// init rethinkdb
	session := dbConnect()
	defer session.Close()
	// init Twitter API
	api := configureTwitterAPI()
	defer api.Close()

	if timeline {
		newTimelineTask(session, api).run(session)
	}

	if timelineCsv {
		newTimelineCsvTask().run(session)
	}

	if followers {
		newFollowerTask(session, api).run(session)
	}
}

func printHelp() {
	flag.PrintDefaults()
	fmt.Println("twitter expects to find Twitter credentials in four environment variables")
	fmt.Println("TWITTER_CONSUMER_KEY")
	fmt.Println("TWITTER_CONSUMER_SECRET")
	fmt.Println("TWITTER_APPLICATION_KEY")
	fmt.Println("TWITTER_APPLICATION_SECRET")
}

type task struct {
	query     r.Term
	extractor func(row map[string]interface{}) interface{}
	results   chan interface{}
	processor func(interface{})
	async     bool
}

func newTask(query r.Term, extractor func(row map[string]interface{}) interface{}, processor func(interface{}), async bool) *task {
	return &task{
		query:     query,
		extractor: extractor,
		results:   make(chan interface{}),
		processor: processor,
		async:     async,
	}
}

func (t *task) run(session *r.Session) {
	go t.entityLoop(session)

	for {
		if entity, ok := <-t.results; ok {
			if t.async {
				go t.processor(entity)
			} else {
				t.processor(entity)
			}
		} else {
			log.Println("No more entities.")
			return
		}
	}
}

// entityLoop fetches all entities returned by the tasks query, passes them to the extractor
// and queues them on the results channel
func (t *task) entityLoop(session *r.Session) {
	log.Println("Executing database query")
	cursor, err := t.query.Run(session)
	if err != nil {
		log.Fatalln(err)
	}
	defer cursor.Close()
	var row map[string]interface{}
	for cursor.Next(&row) {
		t.results <- t.extractor(row)
	}

	if err = cursor.Err(); err != nil {
		log.Fatalln(err)
	}

	close(t.results)
}

func dbConnect() *r.Session {
	log.Println("Connecting to database")
	var session *r.Session

	session, err := r.Connect(r.ConnectOpts{
		Address:  "localhost:28015",
		Database: "angles",
	})

	if err != nil {
		log.Fatalln(err)
	}

	session.Use("angles")
	return session
}

// configureTwitterAPI sets the credentials for the Twitter API
func configureTwitterAPI() *twitter.TwitterApi {
	twitter.SetConsumerKey(os.Getenv(twitterConsumerKey))
	twitter.SetConsumerSecret(os.Getenv(twitterConsumerSecret))

	return twitter.NewTwitterApi(os.Getenv(twitterApplicationKey), os.Getenv(twitterApplicationSecret))
}
