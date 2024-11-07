//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

// Transaction implements a database transaction.
type Transaction struct {
	writable map[PhysicalID]bool
}
