// Copyright (c) 2015 Niklas Wolber
// This file is licensed under the MIT license.
// See the LICENSE file for more information.
package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	twitter "github.com/ChimeraCoder/anaconda"
	r "github.com/dancannon/gorethink"
)

type followerExplorer struct {
	id        int64
	followers []int64
}

func newFollowerTask(session *r.Session, api *twitter.TwitterApi) *task {
	selectUncrawledExplorers := func(row r.Term) interface{} {
		return row.HasFields("followersCrawled").Not()
	}
	query := r.
		Table("explorers").
		Filter(selectUncrawledExplorers)

	extractor := func(row map[string]interface{}) interface{} {
		var exp followerExplorer
		exp.id = int64(row["id"].(float64))
		return exp
	}

	processor := func(entity interface{}) {
		exp := entity.(followerExplorer)
		defer exp.markFollower(session)

		var nextCursor int64
		log.Println("Fetching followers of", exp.id)
		for {
			params := url.Values{}
			params.Add("user_id", fmt.Sprintf("%d", exp.id))
			params.Add("count", "5000")
			if nextCursor > 0 {
				params.Add("cursor", fmt.Sprintf("%d", nextCursor))
			}

			cursor, err := api.GetFollowersIds(params)

			if err != nil {
				log.Println(err)
				switch err := err.(type) {
				case *twitter.ApiError:
					if err.StatusCode >= http.StatusBadRequest {
						log.Println("Abort processing of current user.")
						return
					}
				default:
					log.Println("Sleeping a minute and then try again.")
					time.Sleep(time.Minute)
					continue
				}
			}

			log.Println("Fetched", len(cursor.Ids), "followers of", exp.id)
			exp.storeFollowers(session, cursor.Ids)

			nextCursor = cursor.Next_cursor
			if nextCursor == 0 {
				return
			}
		}
	}
	return newTask(query, extractor, processor, false)
}

func (exp *followerExplorer) storeFollowers(session *r.Session, followers []int64) {
	rows := make([]map[string]interface{}, len(followers))
	for i, follower := range followers {
		rows[i] = map[string]interface{}{
			"explorer": exp.id,
			"follower": follower,
		}
	}
	result, err := r.
		Table("followers").
		Insert(rows).
		RunWrite(session)

	if err != nil {
		log.Println(err)
	}

	if result.Errors > 0 {
		log.Println(result.FirstError)
	}
}

func (exp *followerExplorer) markFollower(session *r.Session) {
	result, err := r.
		Table("explorers").
		Insert(map[string]interface{}{
		"id":               exp.id,
		"followersCrawled": true,
	}, r.InsertOpts{Conflict: "update"}).
		RunWrite(session)

	if err != nil {
		log.Println(err)
	}

	if result.Errors > 0 {
		log.Println(result.FirstError)
	}
}
