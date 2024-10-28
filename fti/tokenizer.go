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

// Tokenizer implements text tokenization.
type Tokenizer struct {
	in        *bufio.Reader
	cvt       func(r rune) rune
	stopwords map[string]bool
	C         chan Token
}

// NewTokenizer creates a new Tokenizer for the input in. The function
// cvt converts runes to the default case and stopwords define the
// stopwords to ignore in tokenization.
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

// Run tokenizes the input.
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

// Token defines an input token.
type Token struct {
	Offset int
	Data   string
}
