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
	"errors"
	"strconv"
	"time"

	"github.com/algotiqa/types"
)

//=============================================================================

const Date = "Date"
const Time = "Time"
const Open = "Open"
const High = "High"
const Low = "Low"
const Close = "Close"
const Up = "Up"
const Down = "Down"

//=============================================================================
//===
//=== Common functions
//===
//=============================================================================

func parseInt(value string, name string) (int, error) {
	res, err := strconv.Atoi(value)

	if err != nil {
		return 0, errors.New("Field '" + name + "' is not a valid integer")
	}

	return res, nil
}

//=============================================================================

func parseFloat(value string, name string) (float64, error) {
	res, err := strconv.ParseFloat(value, 64)

	if err != nil {
		return 0, errors.New("Field '" + name + "' is not a valid float")
	}

	return res, nil
}

//=============================================================================

func parseTimestamp(date string, hhmm string, loc *time.Location) (time.Time, error) {
	year, mon, day, err := parseDate(date)

	if err == nil {
		hh, mm, err := parseTime(hhmm)

		if err == nil {
			return time.Date(year, time.Month(mon), day, hh, mm, 0, 0, loc), nil
		}

		return time.Now(), err
	}

	return time.Now(), err
}

//=============================================================================

func parseDate(date string) (int, int, int, error) {
	if len(date) != 10 || date[2] != '/' || date[5] != '/' {
		return 0, 0, 0, errors.New("Field '" + Date + "' has an invalid format")
	}

	sMon := date[0:2]
	sDay := date[3:5]
	sYear := date[6:]

	if mon, err := strconv.Atoi(sMon); err == nil {
		if day, err := strconv.Atoi(sDay); err == nil {
			if year, err := strconv.Atoi(sYear); err == nil {
				return year, mon, day, nil
			}
		}
	}

	return 0, 0, 0, errors.New("Field '" + Date + "' has an invalid format")
}

//=============================================================================

func parseTime(hhmm string) (int, int, error) {
	if len(hhmm) != 5 || hhmm[2] != ':' {
		return 0, 0, errors.New("Field '" + Time + "' has an invalid format")
	}

	sHH := hhmm[0:2]
	sMM := hhmm[3:]

	if hh, err := strconv.Atoi(sHH); err == nil {
		if mm, err := strconv.Atoi(sMM); err == nil {
			return hh, mm, nil
		}
	}

	return 0, 0, errors.New("field '" + Time + "' has an invalid format")
}

//=============================================================================

func updateDataRange(t time.Time, r *DataRange) {
	y, m, d := t.Date()
	date := types.Date(y*10000 + int(m)*100 + d)

	//--- Handle from day

	if r.FromDay.IsNil() || r.FromDay > date {
		r.FromDay = date
	}

	//--- Handle to day

	if r.ToDay.IsNil() || r.ToDay < date {
		r.ToDay = date
	}
}

//=============================================================================
