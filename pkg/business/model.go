//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package business

import (
	"github.com/algotiqa/data-collector/pkg/core"
	"github.com/algotiqa/data-collector/pkg/db"
	"github.com/algotiqa/data-collector/pkg/ds"
)

//=============================================================================
//===
//=== Upload spec & response
//===
//=============================================================================

type DatafileUploadSpec struct {
	Symbol       string `json:"symbol"       binding:"required"`
	Name         string `json:"name"         binding:"required"`
	FileTimezone string `json:"fileTimezone" binding:"required"`
	Parser       string `json:"parser"       binding:"required"`
}

//=============================================================================

type DatafileUploadResponse struct {
	Duration int   `json:"duration"`
	Bytes    int64 `json:"bytes"`
}

//=============================================================================
//=== Get data request & response
//=============================================================================

type DataInstrumentDataResponse struct {
	Id               uint            `json:"id"`
	Symbol           string          `json:"symbol"`
	From             string          `json:"from"`
	To               string          `json:"to"`
	Timeframe        int             `json:"timeframe"`
	Timezone         string          `json:"timezone"`
	Reduction        int             `json:"reduction,omitempty"`
	Reduced          bool            `json:"reduced"`
	Records          int             `json:"records"`
	NoDataForVirtual bool            `json:"noDataForVirtual"`
	DataPoints       []*ds.DataPoint `json:"dataPoints"`
}

//=============================================================================

type DataInstrumentExt struct {
	db.DataInstrument
}

//=============================================================================
//=== Bias analysis
//=============================================================================

type BiasAnalysisSpec struct {
	DataInstrumentId uint   `json:"dataInstrumentId"`
	BrokerProductId  uint   `json:"brokerProductId"`
	Name             string `json:"name"`
	Notes            string `json:"notes"`
}

//=============================================================================

type BiasAnalysisExt struct {
	db.BiasAnalysis
	DataInstrument db.DataInstrument `json:"dataInstrument"`
	BrokerProduct  db.BrokerProduct  `json:"brokerProduct"`
	Configs        *[]*BiasConfig    `json:"configs"`
}

//=============================================================================

type BiasConfigSpec struct {
	StartDay    int16    `json:"startDay"`
	StartSlot   int16    `json:"startSlot"`
	EndDay      int16    `json:"endDay"`
	EndSlot     int16    `json:"endSlot"`
	Months      []bool   `json:"months"`
	Excludes    []string `json:"excludes"`
	Operation   int8     `json:"operation"`
	GrossProfit float64  `json:"grossProfit"`
	NetProfit   float64  `json:"netProfit"`
}

//-----------------------------------------------------------------------------

func (bcs *BiasConfigSpec) ToBiasConfig() *db.BiasConfig {
	var bc db.BiasConfig
	bc.StartDay = bcs.StartDay
	bc.StartSlot = bcs.StartSlot
	bc.EndDay = bcs.EndDay
	bc.EndSlot = bcs.EndSlot
	bc.Months = core.EncodeMonths(bcs.Months)
	bc.Excludes = core.EncodeExcludes(bcs.Excludes)
	bc.Operation = bcs.Operation
	bc.GrossProfit = bcs.GrossProfit
	bc.NetProfit = bcs.NetProfit

	return &bc
}

//=============================================================================

type BiasConfig struct {
	BiasConfigSpec
	Id             uint `json:"id"`
	BiasAnalysisId uint `json:"biasAnalysisId"`
}

//-----------------------------------------------------------------------------

func (bc *BiasConfig) FromBiasConfig(dbc *db.BiasConfig) {
	bc.Id             = dbc.Id
	bc.BiasAnalysisId = dbc.BiasAnalysisId
	bc.StartDay       = dbc.StartDay
	bc.StartSlot      = dbc.StartSlot
	bc.EndDay         = dbc.EndDay
	bc.EndSlot        = dbc.EndSlot
	bc.Months         = core.DecodeMonths(dbc.Months)
	bc.Excludes       = core.DecodeExcludes(dbc.Excludes)
	bc.Operation      = dbc.Operation
	bc.GrossProfit    = dbc.GrossProfit
	bc.NetProfit      = dbc.NetProfit
}

//=============================================================================
