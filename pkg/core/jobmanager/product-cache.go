//=============================================================================
//===
//=== Copyright (C) 2025-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package jobmanager

import (
	"sync"

	"github.com/algotiqa/data-collector/pkg/db"
)

//=============================================================================

type ProductCache struct {
	sync.RWMutex
	root   string
	blocks map[string]*db.DataBlock
}

//=============================================================================

func NewProductCache(root string) *ProductCache {
	return &ProductCache{
		root:   root,
		blocks: make(map[string]*db.DataBlock),
	}
}

//=============================================================================
//===
//=== API methods
//===
//=============================================================================

func (pc *ProductCache) getDataBlock(symbol string) *db.DataBlock {
	pc.RLock()
	i, found := pc.blocks[symbol]
	pc.RUnlock()

	if found {
		return i
	}

	return nil
}

//=============================================================================

func (pc *ProductCache) addDataBlock(db *db.DataBlock) {
	pc.Lock()
	pc.blocks[db.Symbol] = db
	pc.Unlock()
}

//=============================================================================
