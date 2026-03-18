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

package business

import (
	"errors"
	"strconv"
	"time"

	"github.com/algotiqa/data-collector/pkg/ds"
)

//=============================================================================

type QuerySpec struct {
	Id        uint
	From      string
	To        string
	BackDays  string
	Timezone  string
	Timeframe string
	Reduction string
	Config    *DataConfig
}

//=============================================================================

type QueryParams struct {
	Location   *time.Location
	From       *time.Time
	To         *time.Time
	Reduction  int
	Timeframe  int
	Aggregator ds.DataAggregator
}

//=============================================================================

func NewQueryParams(spec *QuerySpec) (*QueryParams, error) {
	loc, err := getLocation(spec.Timezone, spec.Config)
	if err != nil {
		return nil, errors.New("Bad 'timezone': " + spec.Timezone + " (" + err.Error() + ")")
	}

	backDays, err := parseBackDays(spec.BackDays)
	if err != nil {
		return nil, errors.New("Bad 'backDays': " + spec.BackDays + " (" + err.Error() + ")")
	}

	var from, to *time.Time

	if backDays > 0 {
		now := time.Now()
		back := now.Add(-time.Hour * 24 * time.Duration(backDays))
		from = &back
		to = &now
	} else {
		from, err = parseTime(spec.From, loc)
		if err != nil {
			return nil, errors.New("Bad 'from': " + spec.From + " (" + err.Error() + ")")
		}

		to, err = parseTime(spec.To, loc)
		if err != nil {
			return nil, errors.New("Bad 'to': " + spec.From + " (" + err.Error() + ")")
		}
	}

	timeframe, err := parseTimeframe(spec.Timeframe)
	if err != nil {
		return nil, errors.New("Bad 'timeframe': " + spec.Timeframe + " (" + err.Error() + ")")
	}

	da, err := buildDataAggregator(timeframe)
	if err != nil {
		return nil, errors.New("Bad 'timeframe': " + spec.Timeframe + " (" + err.Error() + ")")
	}

	red, err := parseReduction(spec.Reduction)
	if err != nil {
		return nil, errors.New("Bad 'reduction': " + spec.Reduction + " (" + err.Error() + ")")
	}

	return &QueryParams{
		From:       from,
		To:         to,
		Location:   loc,
		Reduction:  red,
		Timeframe:  timeframe,
		Aggregator: da,
	}, nil
}

//=============================================================================

func getLocation(timezone string, config *DataConfig) (*time.Location, error) {
	if timezone == "" || timezone == "exchange" {
		timezone = config.DataProduct.Timezone
	}

	return time.LoadLocation(timezone)
}

//=============================================================================

func parseBackDays(value string) (int, error) {
	if value == "" {
		return 0, nil
	}

	days, err := strconv.Atoi(value)

	if err != nil {
		return 0, err
	}

	if days < 0 || days > 10000 {
		return 0, errors.New("allowed range is [0..10000]")
	}

	return days, nil
}

//=============================================================================

func parseTimeframe(value string) (int, error) {
	if value == "" {
		return 0, errors.New("value is missing")
	}

	tf, err := strconv.Atoi(value)

	if err != nil {
		return 0, err
	}

	if tf < 1 || tf > 1440 {
		return 0, errors.New("allowed range is [1..1440]")
	}

	return tf, nil
}

//=============================================================================

func parseTime(t string, loc *time.Location) (*time.Time, error) {
	if len(t) == 0 {
		return nil, nil
	}

	date, err := time.ParseInLocation(time.DateTime, t, loc)
	if err == nil {
		date = date.UTC()
	}

	return &date, err
}

//=============================================================================

func parseReduction(value string) (int, error) {
	if value == "" {
		return 0, nil
	}

	red, err := strconv.Atoi(value)

	if err != nil {
		return 0, err
	}

	if red == 0 {
		return red, nil
	}

	if red < 100 || red > 100000 {
		return 0, errors.New("allowed range is [100..100000]")
	}

	return red, nil
}

//=============================================================================

func buildDataAggregator(timeframe int) (ds.DataAggregator, error) {
	tf := timeframe

	if tf == 1 || tf == 5 || tf == 15 || tf == 60 || tf == 1440 {
		return ds.NewSimpleAggregator(ds.NewQuantizerIdentity(tf)), nil
	}

	if tf == 10 {
		return ds.NewSimpleAggregator(ds.NewQuantizer5mTo10m()), nil
	}

	if tf == 30 {
		return ds.NewSimpleAggregator(ds.NewQuantizer15mTo30m()), nil
	}
	//TODO: decide a better strategy. Put into a factory
	return ds.NewSimpleAggregator(ds.NewQuantizer1mToGeneric(tf)), nil
}

//=============================================================================
