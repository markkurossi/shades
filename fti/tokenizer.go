//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package fti

import (
	"bufio"
	"io"
	"unicode"
)

type Tokenizer struct {
	in        *bufio.Reader
	cvt       func(r rune) rune
	stopwords map[string]bool
	C         chan Token
}

func NewTokenizer(in io.Reader, cvt func(r rune) rune,
	stopwords []string) *Tokenizer {

	t := &Tokenizer{
		in:        bufio.NewReader(in),
		cvt:       cvt,
		stopwords: make(map[string]bool),
		C:         make(chan Token),
	}
	for _, word := range stopwords {
		var runes []rune
		for _, r := range word {
			runes = append(runes, cvt(r))
		}
		t.stopwords[string(runes)] = true
	}

	return t
}

func (t *Tokenizer) Run() {
	var ofs int

	for {
		r, s, err := t.in.ReadRune()
		if err != nil {
			break
		}
		start := ofs
		ofs += s
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
			continue
		}
		var runes []rune
		for {
			runes = append(runes, t.cvt(r))

			r, s, err = t.in.ReadRune()
			if err != nil {
				break
			}
			ofs += s
			if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
				break
			}
		}
		word := string(runes)
		_, ok := t.stopwords[word]
		if !ok {
			t.C <- Token{
				Offset: start,
				Data:   word,
			}
		}
	}
	close(t.C)
}

type Token struct {
	Offset int
	Data   string
}
