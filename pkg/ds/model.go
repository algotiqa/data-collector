//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package ds

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

//=============================================================================

type DataPoint struct {
	Time         time.Time `json:"time"`
	Open         float64   `json:"open"`
	High         float64   `json:"high"`
	Low          float64   `json:"low"`
	Close        float64   `json:"close"`
	UpVolume     int       `json:"upVolume"`
	DownVolume   int       `json:"downVolume"`
	UpTicks      int       `json:"upTicks"`
	DownTicks    int       `json:"downTicks"`
	OpenInterest int       `json:"openInterest"`
}

//=============================================================================

func (dp *DataPoint) String() string {
	var sb strings.Builder
	sb.WriteString(dp.Time.String())
	sb.WriteString(",")
	sb.WriteString(fmt.Sprintf("%f", dp.Open))
	sb.WriteString(",")
	sb.WriteString(fmt.Sprintf("%f", dp.High))
	sb.WriteString(",")
	sb.WriteString(fmt.Sprintf("%f", dp.Low))
	sb.WriteString(",")
	sb.WriteString(fmt.Sprintf("%f", dp.Close))
	sb.WriteString(",")
	sb.WriteString(strconv.Itoa(dp.UpVolume))
	sb.WriteString(",")
	sb.WriteString(strconv.Itoa(dp.DownVolume))
	sb.WriteString(",")
	sb.WriteString(strconv.Itoa(dp.UpTicks))
	sb.WriteString(",")
	sb.WriteString(strconv.Itoa(dp.DownTicks))
	sb.WriteString(",")
	sb.WriteString(strconv.Itoa(dp.OpenInterest))

	return sb.String()
}

//=============================================================================

type DataConfig struct {
	UserTable bool
	Selector  any
	Symbol    string
}

//=============================================================================
