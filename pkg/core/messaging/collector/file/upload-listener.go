//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package file

import (
	"log/slog"
	"time"

	"github.com/algotiqa/core/dbms"
	"github.com/algotiqa/data-collector/pkg/business"
	"github.com/algotiqa/data-collector/pkg/core"
	"github.com/algotiqa/data-collector/pkg/db"
	"github.com/algotiqa/data-collector/pkg/ds"
	"github.com/algotiqa/types"
	"gorm.io/gorm"
)

//=============================================================================

func Upload(job *db.IngestionJob) bool {
	//--- Wait 2 secs to allow the commit to complete
	time.Sleep(time.Second * 2)

	slog.Info("HandleFileUpload: Uploading data file into datastore", "filename", job.Filename)
	var context *ParserContext

	block, err := setDataBlockInLoading(job)
	if err == nil {
		if block == nil {
			slog.Warn("HandleFileUpload: Cannot find data block. Maybe the data product has been deleted", "filename", job.Filename, "dbId", job.DataBlockId)
			_ = ds.DeleteDataFile(job.Filename)
			return true
		}

		context, err = ingestDatafile(job, block)
		if err == nil {
			err = setDataBlockInProcessing(job, block, context.DataRange)
			if err == nil {
				slog.Info("HandleFileUpload: Calculating aggregates", "filename", job.Filename)
				err = calcAggregates(context)
				if err == nil {
					err = setDataBlockInReady(block)
					if err == nil {
						slog.Info("HandleFileUpload: Operation complete", "filename", job.Filename)
						_ = ds.DeleteDataFile(job.Filename)
						return true
					}
				}
			}
		}
	}

	slog.Error("HandleFileUpload: Raised error while processing message", "filename", job.Filename, "error", err.Error())
	setJobInError(err, job, block)
	_ = ds.DeleteDataFile(job.Filename)
	return true
}

//=============================================================================

func setDataBlockInLoading(job *db.IngestionJob) (*db.DataBlock, error) {
	var b *db.DataBlock
	var err error

	err = dbms.RunInTransaction(func(tx *gorm.DB) error {
		b, err = db.GetDataBlockById(tx, job.DataBlockId)
		if err != nil {
			return err
		}

		if b != nil {
			b.Status   = db.DBStatusLoading
			b.Progress = 0

			err = db.UpdateDataBlock(tx, b)
		}

		return err
	})

	return b, err
}

//=============================================================================

func ingestDatafile(job *db.IngestionJob, b *db.DataBlock) (*ParserContext, error) {
	start := time.Now()

	parser, err := NewParser(job.Parser)
	if err != nil {
		return nil, err
	}

	//--- This is the file's timezone (will be used to parse dates inside the file)
	fileLoc, err := retrieveLocation(job.Timezone)
	if err != nil {
		return nil, err
	}

	config, err := retrieveConfig(job.DataInstrumentId)
	if err != nil {
		return nil, err
	}

	prodLoc, err := retrieveLocation(config.DataProduct.Timezone)
	if err != nil {
		return nil, err
	}

	file, err := ds.OpenDatafile(job.Filename)
	if err != nil {
		return nil, err
	}

	context := NewParserContext(file, config, fileLoc, job, b, prodLoc)
	defer file.Close()

	err = parser.Parse(context)
	if err != nil {
		slog.Error("ingestDatafile: Parser error --> " + err.Error())
		return nil, err
	}

	//--- Return stats

	end := time.Now()
	dur := end.Sub(start)

	slog.Info("ingestDatafile: Upload complete", "records", job.Records, "duration", dur.Seconds())

	return context, nil
}

//=============================================================================

func retrieveLocation(timezone string) (*time.Location, error) {
	if timezone == "utc" {
		return time.UTC, nil
	}

	return time.LoadLocation(timezone)
}

//=============================================================================

func retrieveConfig(id uint) (*core.QueryConfig, error) {
	var config *core.QueryConfig

	err := dbms.RunInTransaction(func(tx *gorm.DB) error {
		cfg, err := business.CreateQueryConfig(tx, id, "")
		config = cfg
		return err
	})

	return config, err
}

//=============================================================================

func setDataBlockInProcessing(job *db.IngestionJob, b *db.DataBlock, dr *DataRange) error {
	return dbms.RunInTransaction(func(tx *gorm.DB) error {
		if b.DataFrom.IsNil() || b.DataFrom > dr.FromDay {
			b.DataFrom = dr.FromDay
		}

		if b.DataTo.IsNil() || b.DataTo < dr.ToDay {
			b.DataTo = dr.ToDay
		}

		b.Status = db.DBStatusProcessing
		err := db.UpdateDataBlock(tx, b)
		if err != nil {
			return err
		}

		return db.UpdateIngestionJob(tx, job)
	})
}

//=============================================================================

func setDataBlockInReady(block *db.DataBlock) error {
	return dbms.RunInTransaction(func(tx *gorm.DB) error {
		block.Status = db.DBStatusReady
		block.Progress = 100

		return db.UpdateDataBlock(tx, block)
	})
}

//=============================================================================

func setJobInError(err error, job *db.IngestionJob, block *db.DataBlock) {
	_ = dbms.RunInTransaction(func(tx *gorm.DB) error {
		block.Status = db.DBStatusError
		job.Error = err.Error()
		_ = db.UpdateDataBlock(tx, block)
		_ = db.UpdateIngestionJob(tx, job)

		return nil
	})
}

//=============================================================================

func calcAggregates(context *ParserContext) error {
	da5m := context.DataAggreg
	config := context.Config
	session,err := types.NewTradingSession(config.DataProduct.TradingSessionConfig)
	if err != nil {
		return err
	}

	err = ds.BuildAggregates(da5m, config.DataConfig)
	if err == nil {
		da1440m := ds.NewDailyAggregator(session)
		da5m.Aggregate(da1440m)
		err = ds.SaveAggregate(da1440m, config.DataConfig)
	}

	return err
}

//=============================================================================
