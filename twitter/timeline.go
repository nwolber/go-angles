// Copyright (c) 2015 Niklas Wolber
// This file is licensed under the MIT license.
// See the LICENSE file for more information.
package main

import (
	"fmt"
	"log"
	"net/url"

	twitter "github.com/ChimeraCoder/anaconda"
	r "github.com/dancannon/gorethink"
)

type timelineExplorer struct {
	id         int64
	screenName string `gorethink:"screen_name"`
	fetchTime  int64
	maxID      int64
}

func newTimelineTask(session *r.Session, api *twitter.TwitterApi) *task {
	query := r.
		Table("explorers").
		OrderBy(r.OrderByOpts{Index: "fetchTime"})

	extractor := func(row map[string]interface{}) interface{} {
		var exp timelineExplorer
		exp.id = int64(row["id"].(float64))
		exp.screenName = row["screen_name"].(string)
		exp.maxID = int64(row["maxID"].(float64))
		exp.fetchTime = int64(row["fetchTime"].(float64))
		return exp
	}

	processor := func(entity interface{}) {
		exp := entity.(timelineExplorer)
		tweets := exp.getTimeline(api, session)
		for {
			tweet, ok := <-tweets

			if !ok {
				break
			}

			for _, uri := range tweet.Entities.Urls {
				storeURI(session, exp.id, tweet.Id, uri.Expanded_url)
			}
		}
	}
	return newTask(query, extractor, processor, false)
}

// update perists the latest timeline state (i.e. fetchTime and maxID)
func (e *timelineExplorer) update(session *r.Session, maxID *int64) {
	r.
		Table("explorers").
		Update(map[string]interface{}{
		"id":        e.id,
		"fetchTime": r.Now(),
		"maxID":     *maxID,
	}).
		RunWrite(session)
}

// getTimeline returns all tweets of an explorer, which are younger than his maxID
func (e *timelineExplorer) getTimeline(api *twitter.TwitterApi, session *r.Session) chan twitter.Tweet {
	t := make(chan twitter.Tweet)

	log.Println("Fetching", e.screenName)

	go func() {
		// the minimum id, that has been returned
		var minID int64
		defer e.update(session, &minID)

		for {
			queryParams := url.Values{}
			queryParams.Add("screen_name", e.screenName)
			queryParams.Add("count", "200")
			queryParams.Add("trim_user", "true")

			if minID > 0 {
				// max_id returns tweets with IDs less than or equal to max_id.
				// In the case where we reached the oldest tweet, this would
				// return the oldest tweet over and over again. So we decrement
				// the value by one.
				queryParams.Add("max_id", fmt.Sprintf("%d", minID-1))
			}

			// automatically stops and retries, when rate limit is reached
			tweets, err := api.GetUserTimeline(queryParams)

			if err != nil {
				log.Println(err)
				close(t)
				return
			}

			if len(tweets) == 0 {
				log.Println("No more tweets.")
				close(t)
				return
			}

			log.Println("Fetched", len(tweets), "tweets from @", e.screenName)

			// process and deliver tweets
			for _, tweet := range tweets {
				if tweet.Id <= e.maxID {
					// we have seen enough
					log.Println("maxID reached, stopping.")
					close(t)
					return
				}

				if tweet.Id < minID || minID == 0 {
					minID = tweet.Id
				}

				t <- tweet
			}
		}
	}()

	return t
}

// storeURI persists a URI tweeted by an explorer in the database
func storeURI(session *r.Session, explorerID, tweetID int64, uri string) {
	r.
		Table("tweetedUris").
		Insert(map[string]interface{}{
		"explorer": explorerID,
		"tweet":    tweetID,
		"uri":      uri,
	}).
		RunWrite(session)
}
