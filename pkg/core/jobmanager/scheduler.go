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
	"log/slog"
	"time"

	"github.com/algotiqa/data-collector/pkg/db"
)

//=============================================================================

const (
	//--- Value MaxJobs=4 causes TS to raise a "429:Too many requests" error

	MaxJobs = 1
)

type Executor func(ac *AdapterCache, uc *UserConnection) bool
type Resumer func(ac *AdapterCache, uc *UserConnection)

//=============================================================================

var ticker *time.Ticker

//=============================================================================

func startScheduler() {
	ticker = time.NewTicker(1 * time.Second)

	go func() {
		for range ticker.C {
			run()
		}
	}()
}

//=============================================================================

func run() {
	cache.schedule(MaxJobs, executor)
}

//=============================================================================

func executor(ac *AdapterCache, uc *UserConnection) bool {
	jc := NewJobContext(uc, ac, false)
	err := jc.UpdateJob(db.DBStatusLoading, db.DJStatusRunning, "", false)
	if err == nil {
		go func() {
			runJob(jc)
		}()
	}

	return err == nil
}

//=============================================================================

func runJob(jc *JobContext) {
	job := &InstrumentDownLoadJob{}
	err := job.execute(jc)

	if err == nil {
		if jc.sleeping {
			slog.Info("DownloadJob: Job sent in sleeping status", "error", err, "jobId", jc.userConnection.scheduledJob.job.Id)
			err = jc.SleepJob()
		} else {
			err = jc.EndJob()
		}
	} else {
		slog.Error("DownloadJob: Encountered an error. Operation was aborted", "error", err, "jobId", jc.userConnection.scheduledJob.job.Id)
		err = jc.AbortJob(err.Error())
	}

	if err != nil {
		var jobId uint = 0
		sj := jc.userConnection.scheduledJob
		if sj != nil {
			jobId = sj.job.Id
		}

		slog.Error("DownloadJob: Cannot end/abort/sleep a job. It will be restarted", "jobId", jobId, "error", err)
	}
}

//=============================================================================
