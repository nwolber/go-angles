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

type followerExplorer struct {
	id        int64
	followers []int64
}

func newFollowerTask(session *r.Session, api *twitter.TwitterApi) *task {
	selectUncrawledExplorers := func(row r.Term) interface{} {
		return row.HasFields("followers").Not()
	}
	query := r.
		Table("pi").
		Filter(selectUncrawledExplorers).
		OrderBy(r.Desc("tweets"))

	extractor := func(row map[string]interface{}) interface{} {
		var exp followerExplorer
		exp.id = int64(row["explorer"].(float64))
		return exp
	}

	processor := func(entity interface{}) {
		exp := entity.(followerExplorer)

		var followers []int64
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
				log.Fatalln(err)
			}

			log.Println("Fetched", len(cursor.Ids), "followers of", exp.id)
			followers = append(followers, cursor.Ids...)

			nextCursor = cursor.Next_cursor

			if nextCursor == 0 {
				exp.storeFollowers(session, followers)
				return
			}
		}
	}
	return newTask(query, extractor, processor, false)
}

func (exp *followerExplorer) storeFollowers(session *r.Session, followers []int64) {
	result, err := r.
		Table("pi").
		Insert(map[string]interface{}{
		"explorer":  exp.id,
		"followers": followers,
	}, r.InsertOpts{Conflict: "update"}).
		RunWrite(session)

	if err != nil {
		log.Println(err)
	}

	if result.Errors > 0 {
		log.Println(result.FirstError)
	}
}
