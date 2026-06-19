//=============================================================================
//===
//=== Copyright (C) 2025-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package platform

import "time"

//=============================================================================

type InstrumentResponse struct {
	Offset   int          `json:"offset"`
	Limit    int          `json:"limit"`
	OverFlow bool         `json:"overFlow"`
	Result   []Instrument `json:"result"`
}

//=============================================================================

type Instrument struct {
	Name           string     `json:"name"`
	Description    string     `json:"description"`
	Exchange       string     `json:"exchange"`
	Country        string     `json:"country"`
	Root           string     `json:"root"`
	ExpirationDate *time.Time `json:"expirationDate"`
	PointValue     int        `json:"pointValue"`
	MinMove        float64    `json:"minMove"`
	Continuous     bool       `json:"continuous"`
	Month          string     `json:"month"`
}

//=============================================================================

type PriceBars struct {
	Symbol          string      `json:"symbol"`
	Date            int         `json:"date"`
	Days            int         `json:"days"`
	Bars            []*PriceBar `json:"bars"`
	NoData          bool        `json:"noData"`
	Timeout         bool        `json:"timeout"`
	TooManyRequests bool        `json:"tooManyRequests"`
}

//=============================================================================

type PriceBar struct {
	TimeStamp    time.Time
	High         float64
	Low          float64
	Open         float64
	Close        float64
	UpVolume     int
	DownVolume   int
	UpTicks      int
	DownTicks    int
	OpenInterest int
}

//=============================================================================
