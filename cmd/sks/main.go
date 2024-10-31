//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"
	"unicode"

	"github.com/markkurossi/shades/fti"
	"github.com/markkurossi/shades/sse"
)

var stopwords = []string{
	"the",
	"of",
	"to",
	"and",
	"a",
	"in",
	"is",
	"it",
	"you",
	"that",
	"he",
	"was",
	"for",
	"on",
	"are",
	"with",
	"as",
	"I",
	"his",
	"they",
	"be",
	"at",
	"one",
	"have",
	"this",
}

func main() {
	q := flag.String("q", "", "query word")
	v := flag.Bool("v", false, "verbose")
	flag.Parse()

	if len(*q) == 0 {
		log.Fatalf("no query string")
	}

	start := time.Now()

	for _, f := range flag.Args() {
		err := indexFile(f)
		if err != nil {
			fmt.Printf("failed to parse %s: %s\n", f, err)
		}
	}
	now := time.Now()

	fmt.Printf("Indexed %d files in %s\n", len(flag.Args()), now.Sub(start))
	fmt.Printf(" - #tokens  : %v\n", numTokens)
	fmt.Printf(" - #keywords: %v\n", len(db))

	start = now

	// fmt.Printf("db: %v\n", db)
	var setup sse.Setup
	if false {
		setup = sse.SKSSetup
	} else {
		setup = sse.BXTSetup
	}

	impl, err := setup(db)
	if err != nil {
		log.Fatal(err)
	}

	now = time.Now()
	fmt.Printf("EDB Setup in %s\n", now.Sub(start))
	start = now

	query := regexp.MustCompilePOSIX("[[:space:]]+").Split(*q, -1)
	fmt.Printf("query: %v\n", query)

	indices, err := impl.Search(query)
	if err != nil {
		log.Fatal(err)
	}
	now = time.Now()

	fmt.Printf("%d matches in %s\n", len(indices), now.Sub(start))

	if *v {
		for idx, id := range indices {
			if id >= len(sources) {
				log.Fatalf("index %v out of range\n", id)
			}
			fmt.Printf("t[%d]:\t%v\n", idx, sources[id])
		}
	}
}

var (
	sources   []string
	db        = make(map[string][]int)
	numTokens int
)

func indexFile(name string) error {
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()

	ind := len(sources)
	sources = append(sources, name)

	tokenizer := fti.NewTokenizer(file, unicode.ToLower, stopwords)
	go tokenizer.Run()

	m := make(map[string]int)

	for token := range tokenizer.C {
		m[token.Data] = ind
		numTokens++
	}
	for k, v := range m {
		db[k] = append(db[k], v)
	}
	return nil
}
