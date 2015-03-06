// Copyright (c) 2015 Niklas Wolber
// This file is licensed under the MIT license.
// See the LICENSE file for more information.
package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"

	r "github.com/dancannon/gorethink"
)

func newTimelineCsvTask() *task {
	selectValidUris := func(row r.Term) interface{} {
		return row.Field("realUri").Eq("INVALID").Not()
	}
	selectExplorersWithName := func(row r.Term) interface{} {
		return row.HasFields("screen_name")
	}
	query := r.
		Db("angles").
		Table("tweetedUris").
		Filter(selectValidUris).
		EqJoin("explorer", r.Table("explorers")).
		Zip().
		Filter(selectExplorersWithName).
		WithFields("explorer", "screen_name", "realUri")

	extractor := func(row map[string]interface{}) interface{} {
		return row
	}

	file, err := os.Create("pairs.csv")

	if err != nil {
		log.Fatalln(err)
	}

	csv := csv.NewWriter(file)
	csv.Comma = ';'

	processor := func(entity interface{}) {
		row := entity.(map[string]interface{})
		csv.Write([]string{fmt.Sprintf("%d", int64(row["explorer"].(float64))), row["screen_name"].(string), row["realUri"].(string)})
	}

	return newTask(query, extractor, processor, false)
}
