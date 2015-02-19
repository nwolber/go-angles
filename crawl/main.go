// Copyright (c) 2015 Niklas Wolber
// This file is licensed under the MIT license.
// See the LICENSE file for more information.
package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"code.google.com/p/go-uuid/uuid"

	r "github.com/dancannon/gorethink"
)

const (
	numWorkers = 50
)

func main() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	db := initDb()
	defer db.close()

	// signal for workers to exit
	quit := make(chan struct{}, numWorkers)
	// signal for program to halt
	exit := make(chan struct{})

	go func() {
		s := <-sig
		// SIGTERM or SIGINT received
		exit <- struct{}{}
		log.Println("Received", s)
		log.Println("Stopping workers.")
		for j := 0; j < numWorkers; j++ {
			quit <- struct{}{}
		}
	}()

	for {
		db.setup()
		log.Println("Fetching tweets")

		uris := db.getUncrawledUris()
		// signal from workers, that they are done
		done := make(chan struct{})

		for i := 0; i < numWorkers; i++ {
			go func() {
				// always signal that the worker exited
				defer func() { done <- struct{}{} }()

				for {
					uri, ok := <-uris
					if !ok {
						return
					}

					log.Println("Fetching", uri)
					resp, err := http.Head(uri)

					var realURI string
					if err != nil {
						log.Println(err)
						realURI = "INVALID"
					} else {
						func() {
							defer resp.Body.Close()
							realURI = resp.Request.URL.String()
							log.Println("Fetched", realURI)
						}()
					}
					db.storeRealURI(uri, realURI)

					select {
					case <-quit:
						return
					default:
						continue
					}
				}
			}()
		}

		// wait for the first worker to be done
		<-done
		log.Println("First worker exited. Waiting for others.")
		// one worker done, waiting for the others to follow
		allDone := make(chan struct{})
		go func() {
			for i := 0; i < numWorkers-1; i++ {
				<-done
			}
			allDone <- struct{}{}
		}()

		// wait for all workers to exit, timeout after 20 seconds
		select {
		case <-allDone:
			log.Println("All workers are done. Cleaning up.")
		case <-time.Tick(20 * time.Second):
			log.Println("Timed out. Not all workers exited in time. Cleaning up anyways.")
		}

		// do clean up work
		db.merge()
		db.cleanup()

		log.Println("You may see errors, from workers, which are late to the show, now. Sad for them, ok for us. :)")

		select {
		case <-exit:
			return
		case <-time.Tick(15 * time.Minute):
		}
	}
}

type db struct {
	tempTable string
	session   *r.Session
}

// initDb initializes the database connection and sets up a temporary table
func initDb() *db {
	log.Println("Connecting to database")

	session, err := r.Connect(r.ConnectOpts{
		Address:  "localhost:28015",
		Database: "angles",
	})

	if err != nil {
		log.Fatalln(err)
	}

	session.Use("angles")

	return &db{
		session: session,
	}
}

// setup allocates temporary database resources. Call cleanup to dispose them.
func (db *db) setup() {
	// create temporary table to hold results
	log.Println("Creating temporary database table.")
	tempTable := strings.Replace(uuid.New(), "-", "_", -1)
	if _, err := r.Db("angles").TableCreate(tempTable, r.TableCreateOpts{Durability: "soft"}).Run(db.session); err != nil {
		log.Fatalln(err)
	}
	if _, err := r.Table(tempTable).IndexCreate("uri").Run(db.session); err != nil {
		log.Fatalln(err)
	}
	db.tempTable = tempTable
}

func (db *db) getUncrawledUris() chan string {
	uris := make(chan string)
	// prepare ReQL statement
	uncrawled := func(row r.Term) interface{} {
		return row.HasFields("realUri").Not()
	}
	selectURI := func(row r.Term) interface{} {
		return row.Field("uri")
	}
	cursor, err := r.
		Table("tweetedUris").
		Filter(uncrawled).
		Map(selectURI).
		Run(db.session)

	if err != nil {
		log.Fatalln(err)
	}

	// return results
	go func() {
		defer cursor.Close()

		var uri string
		// we only want distinct results
		// we could do this with the ReQL Distinct statement, but with large
		// result sets this will fail (row count > 100.000)
		// since we don't know the result set size before hand,
		// we can't up the limit
		seenURIs := make(map[string]struct{})
		for cursor.Next(&uri) {
			if _, exists := seenURIs[uri]; exists {
				continue
			}

			// mark URI as seen, so we only get distinct URIs
			seenURIs[uri] = struct{}{}
			uris <- uri
		}

		// tell any receivers, that we are done
		close(uris)

		if err = cursor.Err(); err != nil {
			log.Fatalln(err)
		}
	}()

	return uris
}

func (db *db) storeRealURI(uri, realURI string) {
	result, err := r.
		Table(db.tempTable).
		// don't wait for disk to confirm write, instead return as soon as possible
		Insert(map[string]interface{}{"uri": uri, "realUri": realURI}, r.InsertOpts{Durability: "soft"}).
		RunWrite(db.session)

	if err != nil {
		log.Println(err)
	}

	if result.Created == 0 && result.Inserted == 0 {
		log.Println("Insert affected 0 rows, huh?!")
	}

	if erro := result.FirstError; erro != "" {
		log.Fatalln(erro)
	}
}

// merge moves the results from the temporary back into the main table
func (db *db) merge() {
	// merge results back into the original table
	log.Println("Merging temporary results.")
	updatedURIs := r.
		Table(db.tempTable).
		EqJoin("uri", r.Table("tweetedUris"), r.EqJoinOpts{Index: "uri"}).
		Zip()
	r.
		Table("tweetedUris").
		Insert(updatedURIs, r.InsertOpts{Conflict: "replace"}).
		RunWrite(db.session)
}

// cleanup removes temporary database resources
func (db *db) cleanup() {
	log.Println("Cleaning up.")
	// remove temporary table
	r.Db("angles").TableDrop(db.tempTable).Run(db.session)
	log.Println("Done.")
}

// close shuts down the database connection
func (db *db) close() {
	db.session.Close()
}
