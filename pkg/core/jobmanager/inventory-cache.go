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

type InventoryCache struct {
	sync.RWMutex
	adapters map[string]*AdapterCache
}

//=============================================================================

func newInventoryCache() *InventoryCache {
	ic := InventoryCache{
		adapters: make(map[string]*AdapterCache),
	}

	return &ic
}

//=============================================================================
//===
//=== API methods
//===
//=============================================================================

func (ic *InventoryCache) getDataBlock(systemCode, root, symbol string) *db.DataBlock {
	ic.RLock()
	ac, found := ic.adapters[systemCode]
	ic.RUnlock()

	if found {
		return ac.getDataBlock(root, symbol)
	}

	return nil
}

//=============================================================================

func (ic *InventoryCache) addDataBlock(db *db.DataBlock) {
	ac := ic.getOrCreate(db.SystemCode)
	ac.addDataBlock(db)
}

//=============================================================================

func (ic *InventoryCache) addScheduledJob(sj *ScheduledJob) {
	ac := ic.getOrCreate(sj.block.SystemCode)
	ac.addScheduledJob(sj)
}

//=============================================================================

func (ic *InventoryCache) setConnection(systemCode, username, connCode string, connected bool) {
	ac := ic.getOrCreate(systemCode)
	ac.setConnection(username, connCode, connected)
}

//=============================================================================

func (ic *InventoryCache) disconnectAll() {
	ic.RLock()

	for _, ac := range ic.adapters {
		ac.disconnectAll()
	}

	ic.RUnlock()
}

//=============================================================================

func (ic *InventoryCache) schedule(maxJobs int, e Executor) {
	ic.RLock()
	ic.RUnlock()

	for _, ac := range ic.adapters {
		maxJobs = ac.schedule(maxJobs, e)

		if maxJobs == 0 {
			return
		}
	}
}

//=============================================================================

func (ic *InventoryCache) cancelUserJobsOnProduct(username, systemCode, root string) bool {
	ic.Lock()
	ac, found := ic.adapters[systemCode]
	ic.Unlock()

	if found {
		return ac.cancelUserJobsOnProduct(username, root)
	}

	return false
}

//=============================================================================
//===
//=== Private methods
//===
//=============================================================================

func (ic *InventoryCache) getOrCreate(systemCode string) *AdapterCache {
	ic.Lock()

	ac, found := ic.adapters[systemCode]
	if !found {
		ac = NewAdapterCache(systemCode)
		ic.adapters[systemCode] = ac
	}

	ic.Unlock()
	return ac
}

//=============================================================================
