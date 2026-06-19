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

	"github.com/algotiqa/core/dbms"
	"github.com/algotiqa/core/msg"
	"github.com/algotiqa/data-collector/pkg/core/messaging/collector/rollover"
	"github.com/algotiqa/data-collector/pkg/db"
	"gorm.io/gorm"
)

//=============================================================================

type Job interface {
	execute(jc *JobContext) error
}

//=============================================================================

type JobContext struct {
	userConnection *UserConnection
	cache          *AdapterCache
	resuming       bool
	sleeping       bool
}

//=============================================================================

func NewJobContext(uc *UserConnection, cache *AdapterCache, resuming bool) *JobContext {
	return &JobContext{
		userConnection: uc,
		cache         : cache,
		resuming      : resuming,
	}
}

//=============================================================================

func (jc *JobContext) GoToSleep() {
	jc.sleeping = true
}

//=============================================================================

func (jc *JobContext) UpdateJob(blkStatus db.DBStatus, jobStatus db.DJStatus, jobErr string, sendRecalcMsg bool) error {
	return dbms.RunInTransaction(func(tx *gorm.DB) error {
		blk := jc.userConnection.scheduledJob.block
		job := jc.userConnection.scheduledJob.job

		oldBlkStatus := blk.Status
		oldJobStatus := job.Status

		blk.Status = blkStatus
		job.Status = jobStatus
		job.Error = jobErr

		err := db.UpdateDataBlock(tx, blk)
		if err == nil {
			err = db.UpdateDownloadJob(tx, job)
			if err == nil && sendRecalcMsg {
				err = jc.sendRollRecalcMessage(tx)
			}
		}

		if err != nil {
			blk.Status = oldBlkStatus
			job.Status = oldJobStatus
			job.Error = ""
		}

		return err
	})
}

//=============================================================================

func (jc *JobContext) EndJob() error {
	err := dbms.RunInTransaction(func(tx *gorm.DB) error {
		blk := jc.userConnection.scheduledJob.block
		job := jc.userConnection.scheduledJob.job

		oldBlk := *blk

		blk.Status = db.DBStatusReady

		if blk.DataFrom.IsNil() || blk.DataTo.IsNil() {
			blk.Status = db.DBStatusEmpty
		}

		err := db.UpdateDataBlock(tx, blk)
		if err == nil {
			err = db.DeleteDownloadJob(tx, job.Id)
			if err == nil {
				err = jc.sendRollRecalcMessage(tx)
			}
		}

		if err != nil {
			blk.Status = oldBlk.Status
			blk.DataFrom = oldBlk.DataFrom
			blk.DataTo = oldBlk.DataTo
			job.UserConnection = ""
		}

		return err
	})

	jc.cache.freeConnection(jc.userConnection, err != nil)

	return err
}

//=============================================================================

func (jc *JobContext) AbortJob(jobErr string) error {
	jc.userConnection.scheduledJob.job.UserConnection = ""
	err := jc.UpdateJob(db.DBStatusError, db.DJStatusError, jobErr, false)

	now := time.Now()
	jc.userConnection.scheduledJob.lastError = &now

	jc.cache.freeConnection(jc.userConnection, true)

	return err
}

//=============================================================================

func (jc *JobContext) SleepJob() error {
	jc.userConnection.scheduledJob.job.UserConnection = ""
	err := jc.UpdateJob(db.DBStatusSleeping, db.DJStatusWaiting, "", true)

	jc.cache.freeConnection(jc.userConnection, true)

	return err
}

//=============================================================================

func (jc *JobContext) sendRollRecalcMessage(tx *gorm.DB) error {
	sj := jc.userConnection.scheduledJob

	job := &rollover.RecalcJob{
		DataBlockId: sj.block.Id,
	}

	err := msg.SendMessage(msg.ExCollector, msg.SourceRollRecalcJob, msg.TypeCreate, job, tx)

	if err != nil {
		slog.Error("sendRollRecalcJobMessage: Could not publish the upload message", "error", err.Error())
	}

	return err
}

//=============================================================================
