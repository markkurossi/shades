//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

// Params define the database parameters.
type Params struct {
	PageSize int
}

// NewParams creates a new parameter object with the system default
// values.
func NewParams() Params {
	return Params{
		PageSize: 16 * 1024,
	}
}
