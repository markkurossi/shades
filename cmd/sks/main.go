//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
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

	for _, f := range flag.Args() {
		err := indexFile(f)
		if err != nil {
			fmt.Printf("failed to parse %s: %s\n", f, err)
		}
	}

	fmt.Printf("Indexed %d files\n", len(flag.Args()))
	fmt.Printf(" - #tokens  : %v\n", numTokens)
	fmt.Printf(" - #keywords: %v\n", len(db))

	// fmt.Printf("db: %v\n", db)

	var ks [16]byte
	_, err := rand.Read(ks[:])
	if err != nil {
		log.Fatal(err)
	}

	sks, err := sse.SKSSetup(ks[:], db)
	if err != nil {
		log.Fatal(err)
	}

	query := []byte(*q)

	indices, err := sks.Search(query)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d matches\n", len(indices))

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
