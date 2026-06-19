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
	"time"

	"github.com/algotiqa/data-collector/pkg/db"
	"github.com/algotiqa/data-collector/pkg/ds"
	"github.com/algotiqa/data-collector/pkg/platform"
	"github.com/algotiqa/types"
)

//=============================================================================

type InstrumentDownLoadJob struct {
}

//=============================================================================

func (i *InstrumentDownLoadJob) execute(jc *JobContext) error {
	uc := jc.userConnection
	sj := uc.scheduledJob
	blk := sj.block
	job := sj.job

	slog.Info("DownloadJob: Starting job", "systemCode", blk.SystemCode, "root", blk.Root, "symbol", blk.Symbol, "jobId", job.Id, "resuming", jc.resuming)

	prodLoc, err := time.LoadLocation(job.ProductTimezone)
	if err != nil {
		return err
	}

	session, err := types.NewTradingSession(job.Session)
	if err != nil {
		return err
	}

	for job.LoadFrom <= job.LoadTo {
		days, errx := processDays(jc, uc, blk, job, prodLoc)
		if errx != nil {
			return errx
		}

		job.LoadFrom = job.LoadFrom.AddDays(days)
		today := types.Today(time.UTC)

		if job.LoadFrom.Days(today) <= 0 {
			//--- We will pass beyond today by 1 day, so we have to re-set LoadFrom
			job.LoadFrom = today
			jc.GoToSleep()
			return recalcDailyBars(blk, session, prodLoc)
		}

		if days == 0 {
			//--- Some days can be missing: we go to the next day anyway
			job.LoadFrom = job.LoadFrom.AddDays(1)

			//--- Let's also wait a little bit because queries with no data are fast and some
			//--- data providers can complain if we send too many requests per minute

			time.Sleep(time.Millisecond * 500)
		}

		if sj.IsCancelled() {
			slog.Info("DownloadJob: Job cancelled", "systemCode", blk.SystemCode, "root", blk.Root, "symbol", blk.Symbol, "jobId", job.Id)
			return nil
		}
	}

	err = recalcDailyBars(blk, session, prodLoc)
	slog.Info("DownloadJob: Ending job", "systemCode", blk.SystemCode, "root", blk.Root, "symbol", blk.Symbol, "jobId", job.Id)
	return err
}

//=============================================================================

func processDays(jc *JobContext, uc *UserConnection, blk *db.DataBlock, job *db.DownloadJob,
	prodLoc *time.Location) (int, error) {
	loadedDays := 0

	bars, err := platform.GetPriceBars(uc.username, uc.connectionCode, blk.Symbol, job.LoadFrom)
	if err == nil {
		if bars.Timeout {
			err = errors.New("Timeout")
		} else if bars.TooManyRequests {
			err = errors.New("Too Many Requests")
		} else {
			job.CurrDay += bars.Days
			loadedDays = bars.Days

			if !bars.NoData {
				var firstDate, lastDate types.Date
				err, firstDate, lastDate = storeBars(blk, bars.Bars, prodLoc)
				if err == nil {
					err = updateStatus(jc, blk, job, firstDate, lastDate)
				}
			}
		}
	}

	if err != nil {
		slog.Error("DownloadJob: Got an error while processing days", "error", err,
			"symbol", blk.Symbol, "loadFrom", job.LoadFrom, "jobId", job.Id)
	}

	return loadedDays, err
}

//=============================================================================

func storeBars(blk *db.DataBlock, bars []*platform.PriceBar, prodLoc *time.Location) (error, types.Date, types.Date) {
	var dataPoints []*ds.DataPoint
	var dataAggreg = ds.NewSimpleAggregator(ds.NewQuantizer1mTo5m())

	config := ds.NewDataConfig(blk.SystemCode, blk.Symbol)

	var firstDate, lastDate types.Date

	for _, bar := range bars {
		lastDate = types.ToDate(&bar.TimeStamp)
		if firstDate.IsNil() {
			firstDate = lastDate
		}

		dp := &ds.DataPoint{
			//--- We need to store in product location because we will calculate daily bars
			Time:         bar.TimeStamp.In(prodLoc),
			Open:         bar.Open,
			High:         bar.High,
			Low:          bar.Low,
			Close:        bar.Close,
			UpVolume:     bar.UpVolume,
			DownVolume:   bar.DownVolume,
			UpTicks:      bar.UpTicks,
			DownTicks:    bar.DownTicks,
			OpenInterest: bar.OpenInterest,
		}

		dataPoints = append(dataPoints, dp)
		dataAggreg.Add(dp)
	}

	err := ds.SetDataPoints(dataPoints, "1m", config)
	if err != nil {
		return err, firstDate, lastDate
	}

	dataAggreg.Flush()
	return ds.BuildAggregates(dataAggreg, config), firstDate, lastDate
}

//=============================================================================

func updateStatus(jc *JobContext, blk *db.DataBlock, job *db.DownloadJob, firstDate, lastDate types.Date) error {
	if blk.DataFrom.IsNil() || blk.DataFrom > firstDate {
		blk.DataFrom = firstDate
	}

	if blk.DataTo.IsNil() || blk.DataTo < lastDate {
		blk.DataTo = lastDate
	}

	blk.Progress = min(int8(job.CurrDay*100/job.TotDays), 100)

	return jc.UpdateJob(db.DBStatusLoading, db.DJStatusRunning, "", false)
}

//=============================================================================

func recalcDailyBars(blk *db.DataBlock, session *types.TradingSession, prodLoc *time.Location) error {
	config := ds.NewDataConfig(blk.SystemCode, blk.Symbol)
	da5m   := ds.NewIdentityAggregator(5)

	err := ds.GetDataPoints(nil, nil, config, prodLoc, da5m, 0)
	if err != nil {
		return err
	}

	da1440m := ds.NewDailyAggregator(session)
	da5m.Aggregate(da1440m)
	return ds.SaveAggregate(da1440m, config)
}

//=============================================================================
