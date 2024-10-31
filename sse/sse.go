//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

var (
	_ Setup = SKSSetup
	_ SSE   = &SKS{}
	_ Setup = BXTSetup
	_ SSE   = &BXT{}
)

type Setup func(db map[string][]int) (SSE, error)

type SSE interface {
	Search(query []string) ([]int, error)
}
