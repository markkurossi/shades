//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package main

import (
	"crypto/rand"
	"encoding/binary"
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

	// fmt.Printf("db: %v\n", db)

	var ks [16]byte
	_, err := rand.Read(ks[:])
	if err != nil {
		log.Fatal(err)
	}

	T, err := sse.EDBSetup(ks[:], db)
	if err != nil {
		log.Fatal(err)
	}
	if false {
		for w, indices := range T {
			fmt.Printf("T[%s]:\n", w)
			for idx, i := range indices {
				fmt.Printf(" %d) %x\n", idx, i)
			}
		}
	}
	tset, err := sse.TSetSetup(T)
	if err != nil {
		log.Fatal(err)
	}
	_ = tset

	query := []byte(*q)

	stag, err := tset.GetTag(query, nil)
	if err != nil {
		log.Fatal(err)
	}

	t, err := tset.Retrieve(stag)
	if err != nil {
		log.Fatal(err)
	}

	prf, err := sse.NewPRF(ks[:])
	if err != nil {
		log.Fatal(err)
	}
	_, err = prf.Write(query)
	if err != nil {
		log.Fatal(err)
	}
	ke := prf.Sum(nil)

	dec, err := sse.NewENC(ke)
	if err != nil {
		log.Fatal(err)
	}

	for idx, id := range t {
		var plain sse.ID
		dec.Decrypt(plain[:], id[:])
		index := int(plain.Uint64())
		if index >= len(sources) {
			log.Fatalf("index %v out of range\n", index)
		}
		fmt.Printf("t[%d]:\t%x\t%v\n", idx, id, sources[index])
	}
}

var (
	sources []string
	db      = make(map[string][]int)
	bo      = binary.BigEndian
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
	}
	for k, v := range m {
		db[k] = append(db[k], v)
	}
	return nil
}
