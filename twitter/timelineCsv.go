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
	query := r.
		Db("angles").
		Table("tweetedUris").
		Filter(selectValidUris).
		WithFields("explorer", "realUri")

	extractor := func(row map[string]interface{}) interface{} {
		return row
	}

	file, err := os.Create("pairs.csv")

	if err != nil {
		log.Fatalln(err)
	}

	csv := csv.NewWriter(file)

	processor := func(entity interface{}) {
		row := entity.(map[string]interface{})
		csv.Write([]string{fmt.Sprintf("%d", int64(row["explorer"].(float64))), row["realUri"].(string)})
	}

	return newTask(query, extractor, processor, true)
}
