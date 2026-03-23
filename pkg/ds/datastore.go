//=============================================================================
/*
Copyright © 2024 Andrea Carboni andrea.carboni71@gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
//=============================================================================

package ds

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/algotiqa/core"
	"github.com/algotiqa/core/req"
	"github.com/algotiqa/data-collector/pkg/app"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//=============================================================================

var DefaultFrom = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
var DefaultTo = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)

//=============================================================================

var pool *pgxpool.Pool
var staging string

type Formatter func(dp *DataPoint) any

//=============================================================================

func InitDatastore(cfg *app.Datastore) {

	slog.Info("Starting datastore...")
	url := "postgres://" + cfg.Username + ":" + cfg.Password + "@" + cfg.Address + "/" + cfg.Name

	p, err := pgxpool.New(context.Background(), url)
	if err != nil {
		core.ExitWithMessage("Failed to connect to the datastore: " + err.Error())
	}

	pool = p
	staging = cfg.Staging
}

//=============================================================================
//===
//=== Datafile management
//===
//=============================================================================

func OpenDatafile(filename string) (*os.File, error) {
	return os.Open(staging + string(os.PathSeparator) + filename)
}

//=============================================================================

func SaveDatafile(part io.Reader) (string, int64, error) {
	var bytes int64
	filename := uuid.NewString()
	slog.Info("Starting datafile upload", "filename", filename)

	file, err := os.Create(staging + string(os.PathSeparator) + filename)
	if err == nil {
		w := bufio.NewWriter(file)
		bytes, err = io.Copy(w, part)

		if err == nil {
			err = w.Flush()
			if err == nil {
				err = file.Close()
				if err == nil {
					slog.Info("Ending datafile upload", "filename", filename, "bytes", bytes)
					return filename, bytes, nil
				}
			}
		}

		_ = file.Close()
		_ = os.Remove(filename)
	}

	slog.Info("Error during datafile upload", "filename", filename, "error", err.Error())
	return "", 0, err
}

//=============================================================================

func DeleteDataFile(filename string) error {
	return os.Remove(staging + string(os.PathSeparator) + filename)
}

//=============================================================================
//===
//=== Datapoints get/set
//===
//=============================================================================

func NewDataConfig(systemCode, symbol string) *DataConfig {
	return &DataConfig{
		UserTable: false,
		Selector:  systemCode,
		Symbol:    symbol,
	}
}

//=============================================================================

func GetDataPoints(from *time.Time, to *time.Time, config *DataConfig, prodLoc *time.Location, da DataAggregator) error {
	if from == nil {
		from = &DefaultFrom
	}
	if to == nil {
		to = &DefaultTo
	}

	query := buildGetQuery(da.BaseTimeframe(), config)

	rows, err := pool.Query(context.Background(), query, config.Symbol, config.Selector, from, to)
	if err != nil {
		return req.NewServerErrorByError(err)
	}

	defer rows.Close()

	for rows.Next() {
		var dp DataPoint
		err = rows.Scan(&dp.Time, &dp.Open, &dp.High, &dp.Low, &dp.Close, &dp.UpVolume, &dp.DownVolume, &dp.UpTicks, &dp.DownTicks, &dp.OpenInterest)

		if err != nil {
			return req.NewServerErrorByError(err)
		}

		//--- Call aggregator in data product's timezone so the aggregator can work with the session
		dp.Time = dp.Time.In(prodLoc)
		da.Add(&dp)
	}

	da.Flush()

	if rows.Err() != nil {
		return req.NewServerErrorByError(rows.Err())
	}

	return nil
}

//=============================================================================

func SetDataPoints(points []*DataPoint, timeframe string, config *DataConfig) error {
	if len(points) == 0 {
		return nil
	}

	query := buildAddQuery(timeframe, config)
	batch := &pgx.Batch{}

	for i := range points {
		dp := points[i]
		batch.Queue(query, dp.Time, config.Symbol, config.Selector, dp.Open, dp.High, dp.Low, dp.Close,
			dp.UpVolume, dp.DownVolume, dp.UpTicks, dp.DownTicks, dp.OpenInterest)
	}

	br := pool.SendBatch(context.Background(), batch)
	_, err := br.Exec()
	_ = br.Close()

	return err
}

//=============================================================================

func BuildAggregates(da5m DataAggregator, config *DataConfig) error {
	err := SaveAggregate(da5m, config)

	if err == nil {
		da15m := NewSimpleAggregator(NewQuantizer5mTo15m())
		da5m.Aggregate(da15m)
		err = SaveAggregate(da15m, config)
		if err == nil {
			da60m := NewSimpleAggregator(NewQuantizer15mTo60m())
			da15m.Aggregate(da60m)
			err = SaveAggregate(da60m, config)
		}
	}

	return err
}

//=============================================================================

func SaveAggregate(da DataAggregator, config *DataConfig) error {
	var dataPoints []*DataPoint

	for _, dp := range da.DataPoints() {
		dataPoints = append(dataPoints, dp)

		if len(dataPoints) == 8192 {
			if err := SetDataPoints(dataPoints, da.TargetTimeframe(), config); err != nil {
				return err
			}
			dataPoints = []*DataPoint{}
		}
	}

	return SetDataPoints(dataPoints, da.TargetTimeframe(), config)
}

//=============================================================================
//===
//=== Private methods
//===
//=============================================================================

func buildGetQuery(timeframe string, config *DataConfig) string {
	table := "system_data_"
	field := "system_code"

	if config.UserTable {
		table = "user_data_"
		field = "product_id"
	}

	table = table + timeframe

	query := "SELECT time, open, high, low, close, up_volume, down_volume, up_ticks, down_ticks, open_interest FROM " + table + " " +
		"WHERE symbol = $1 AND " + field + " = $2 AND time >= $3 AND time <= $4 " +
		"ORDER BY time"

	return query
}

//=============================================================================

func buildAddQuery(timeframe string, config *DataConfig) string {
	table := "system_data_"
	field := "system_code"

	if config.UserTable {
		table = "user_data_"
		field = "product_id"
	}

	table = table + timeframe

	query := "INSERT INTO " + table + "(time, symbol, " + field + ", open, high, low, close, up_volume, down_volume, up_ticks, down_ticks, open_interest) " +
		"VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) " +
		"ON CONFLICT(time, symbol, " + field + ") DO UPDATE SET " +
		"open=excluded.open," +
		"high=excluded.high," +
		"low=excluded.low," +
		"close=excluded.close," +
		"up_volume=excluded.up_volume," +
		"down_volume=excluded.down_volume," +
		"up_ticks=excluded.up_ticks," +
		"down_ticks=excluded.down_ticks," +
		"open_interest=excluded.open_interest"

	return query
}

//=============================================================================
