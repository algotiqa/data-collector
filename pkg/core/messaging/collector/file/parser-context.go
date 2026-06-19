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
	"io"
	"time"

	"github.com/algotiqa/core/dbms"
	"github.com/algotiqa/data-collector/pkg/core"
	"github.com/algotiqa/data-collector/pkg/db"
	"github.com/algotiqa/data-collector/pkg/ds"
	"gorm.io/gorm"
)

//=============================================================================

type ParserContext struct {
	Reader          io.Reader
	Config          *core.QueryConfig
	FileLocation    *time.Location
	ProductLocation *time.Location
	Job             *db.IngestionJob
	Block           *db.DataBlock
	DataRange       *DataRange
	DataAggreg      ds.DataAggregator

	//--- Private stuff

	dataPoints []*ds.DataPoint
	currBytes  int64
}

//=============================================================================
//===
//=== Constructor
//===
//=============================================================================

func NewParserContext(file io.Reader, config *core.QueryConfig, fileLoc *time.Location,
	job *db.IngestionJob, b *db.DataBlock, prodLoc *time.Location) *ParserContext {
	c := &ParserContext{
		Reader:          file,
		Config:          config,
		FileLocation:    fileLoc,
		ProductLocation: prodLoc,
		Job:             job,
		Block:           b,
	}

	c.dataPoints = []*ds.DataPoint{}
	c.DataRange = &DataRange{}
	c.DataAggreg = ds.NewSimpleAggregator(ds.NewQuantizer1mTo5m())

	return c
}

//=============================================================================
//===
//=== Public methods
//===
//=============================================================================

func (c *ParserContext) SaveDataPoint(dp *ds.DataPoint, bytes int) error {
	dp.Time = dp.Time.In(c.ProductLocation)
	c.dataPoints = append(c.dataPoints, dp)
	c.Job.Records++
	c.currBytes += int64(bytes)

	if c.Job.Records%8192 == 0 {
		if err := ds.SetDataPoints(c.dataPoints, "1m", c.Config.DataConfig); err != nil {
			return err
		}
		c.dataPoints = []*ds.DataPoint{}
	}

	updateDataRange(dp.Time, c.DataRange)
	c.DataAggreg.Add(dp)

	return c.updateProgress()
}

//=============================================================================

func (c *ParserContext) Flush() error {
	c.DataAggreg.Flush()
	return ds.SetDataPoints(c.dataPoints, "1m", c.Config.DataConfig)
}

//=============================================================================
//===
//=== Private methods
//===
//=============================================================================

func (c *ParserContext) updateProgress() error {
	curProgress := int8(c.currBytes * 100 / c.Job.Bytes)

	if c.Block.Progress != curProgress {
		c.Block.Progress = curProgress

		return dbms.RunInTransaction(func(tx *gorm.DB) error {
			err := db.UpdateDataBlock(tx, c.Block)
			if err != nil {
				return err
			}

			return db.UpdateIngestionJob(tx, c.Job)
		})
	}

	return nil
}

//=============================================================================
