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
	"errors"
	"log/slog"
	"os"
	"strconv"

	"github.com/algotiqa/core/dbms"
	"github.com/algotiqa/data-collector/pkg/app"
	"github.com/algotiqa/data-collector/pkg/db"
	"gorm.io/gorm"
)

//=============================================================================

var cache *InventoryCache = newInventoryCache()

//=============================================================================

func Init(cfg *app.Config) {
	slog.Info("JobManager: Initializing cache...")

	err := initCache()
	if err != nil {
		slog.Error("Fatal: Cannot initialize Job manager. ", "error", err.Error())
		os.Exit(1)
	}

	startScheduler()
}

//=============================================================================
//===
//=== Public functions
//===
//=============================================================================

func NewScheduledJob(username string, block *db.DataBlock, job *db.DownloadJob) *ScheduledJob {
	sj := &ScheduledJob{}
	sj.username = username
	sj.block    = block
	sj.job      = job

	return sj
}

//=============================================================================

func GetDataBlock(systemCode, root, symbol string) *db.DataBlock {
	return cache.getDataBlock(systemCode, root, symbol)
}

//=============================================================================

func AddScheduledJob(job *ScheduledJob) {
	cache.addScheduledJob(job)
}

//=============================================================================

func AddScheduledJobs(jobs []*ScheduledJob) {
	for _, job := range jobs {
		cache.addScheduledJob(job)
	}
}

//=============================================================================

func SetConnection(systemCode, username, connCode string, connected bool) {
	cache.setConnection(systemCode, username, connCode, connected)
}

//=============================================================================

func DisconnectAll() {
	cache.disconnectAll()
}

//=============================================================================

func CancelUserJobsOnProduct(username, systemCode, root string) bool {
	return cache.cancelUserJobsOnProduct(username, systemCode, root)
}

//=============================================================================
//===
//=== Private functions
//===
//=============================================================================

func initCache() error {
	return dbms.RunInTransaction(func(tx *gorm.DB) error {
		blocksMap, err := loadDataBlocks(tx)
		if err != nil {
			return err
		}

		err = loadDataProducts(tx)
		if err != nil {
			return err
		}

		err = loadDownloadJobs(tx, blocksMap)
		if err != nil {
			return err
		}

		return nil
	})
}

//=============================================================================

func loadDataBlocks(tx *gorm.DB) (map[uint]*db.DataBlock, error) {
	list, err := db.GetGlobalDataBlocks(tx)
	if err != nil {
		return nil, err
	}

	for _, blk := range *list {
		cache.addDataBlock(&blk)

		if blk.Status == db.DBStatusLoading || blk.Status == db.DBStatusProcessing {
			blk.Status = db.DBStatusWaiting
			err = db.UpdateDataBlock(tx, &blk)
			if err != nil {
				return nil, err
			}
		}
	}

	blockMap := convertToMap(list)

	return blockMap, nil
}

//=============================================================================

func convertToMap(list *[]db.DataBlock) map[uint]*db.DataBlock {
	res := make(map[uint]*db.DataBlock)

	for _, b := range *list {
		res[b.Id] = &b
	}

	return res
}

//=============================================================================

func loadDataProducts(tx *gorm.DB) error {
	filter := map[string]any{
		"supports_multiple_data": false,
	}
	products, err := db.GetDataProducts(tx, filter, 0, 5000)
	if err == nil {
		for _, dp := range *products {
			cache.setConnection(dp.SystemCode, dp.Username, dp.ConnectionCode, dp.Connected)
		}
	}

	return err
}

//=============================================================================

func loadDownloadJobs(tx *gorm.DB, blocksMap map[uint]*db.DataBlock) error {
	jobs, err := db.GetDownloadJobs(tx)
	if err == nil {
		for _, job := range *jobs {
			block, found := blocksMap[job.DataBlockId]
			if !found {
				return errors.New("DataBlock not found! --> id:" + strconv.Itoa(int(job.DataBlockId)))
			}

			if job.Status == db.DJStatusRunning {
				job.Status = db.DJStatusWaiting
				err = db.UpdateDownloadJob(tx, &job.DownloadJob)
				if err != nil {
					return err
				}
			}

			sj := NewScheduledJob(job.Username, block, &job.DownloadJob)
			cache.addScheduledJob(sj)
		}
	}

	return err
}

//=============================================================================
