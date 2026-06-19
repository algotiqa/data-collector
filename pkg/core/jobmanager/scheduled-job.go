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
	"time"

	"github.com/algotiqa/data-collector/pkg/db"
	"github.com/algotiqa/types"
)

//=============================================================================

const (
	RetryDelayHours = 1
)

//=============================================================================

type ScheduledJob struct {
	sync.RWMutex
	username  string
	block     *db.DataBlock
	job       *db.DownloadJob
	lastError *time.Time
	cancelled bool
}

//=============================================================================

func (sj *ScheduledJob) IsSchedulable() bool {
	if sj.lastError != nil {
		duration := time.Now().Sub(*sj.lastError) / time.Hour
		return duration > RetryDelayHours
	}

	return sj.job.LoadFrom < types.Today(time.UTC)
}

//=============================================================================

func (sj *ScheduledJob) Cancel() {
	sj.Lock()
	defer sj.Unlock()

	sj.cancelled = true
}

//=============================================================================

func (sj *ScheduledJob) IsCancelled() bool {
	sj.RLock()
	defer sj.RUnlock()

	return sj.cancelled
}

//=============================================================================
