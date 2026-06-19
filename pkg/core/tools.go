//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package core

import (
	"strings"

	"github.com/algotiqa/data-collector/pkg/db"
	"github.com/algotiqa/data-collector/pkg/ds"
	"github.com/algotiqa/types"
)

//=============================================================================

type QueryConfig struct {
	DataConfig     *ds.DataConfig
	DataProduct    *db.DataProduct
	DataInstrument *db.DataInstrument
	Instruments    *[]db.DataInstrument
	TradingSession *types.TradingSession
}

//=============================================================================
//===
//=== BiasConfig encoding/decoding
//===
//=============================================================================

func EncodeMonths(months []bool) int16 {
	var value int16

	if months != nil && len(months) == 12 {
		for _, month := range months {
			value <<= 1
			if month {
				value |= 1
			}
		}
	}

	return value
}

//=============================================================================

func EncodeExcludes(list []string) string {
	var sb strings.Builder

	if list != nil {
		for i, exc := range list {
			if i != 0 {
				sb.WriteString("|")
			}

			sb.WriteString(exc)
		}
	}

	return sb.String()
}

//=============================================================================

func DecodeMonths(value int16) []bool {
	var list []bool
	var bit int16 = 1<<11

	for i:=0; i<12; i++ {
		month := (value & bit) != 0
		list = append(list, month)
		bit >>=1
	}

	return list
}

//=============================================================================

func DecodeExcludes(value string) []string {
	if len(value) == 0 {
		return []string{}
	}

	return strings.Split(value, "|")
}

//=============================================================================

func Trunc2d(value float64) float64 {
	return float64(int(value * 100)) / 100
}

//=============================================================================

func Trunc4d(value float64) float64 {
	return float64(int(value * 10000)) / 10000
}

//=============================================================================
//===
//=== Query config & trading session
//===
//=============================================================================

func NewQueryConfig(i *db.DataInstrument, p *db.DataProduct, instruments *[]db.DataInstrument,
					session *types.TradingSession) *QueryConfig {
	var selector any
	var userTable bool

	if p.SupportsMultipleData {
		userTable = true
		selector = i.Id
	} else {
		userTable = false
		selector = p.SystemCode
	}

	return &QueryConfig{
		DataConfig: &ds.DataConfig{
			UserTable: userTable,
			Selector : selector,
			Symbol   : i.Symbol,
		},
		DataProduct   : p,
		DataInstrument: i,
		Instruments   : instruments,
		TradingSession: session,
	}
}

//=============================================================================

func GetTradingSession(sessionConfig string, dp *db.DataProduct) (*types.TradingSession, error) {
	if sessionConfig == "" {
		sessionConfig = dp.TradingSessionConfig
	}

	return types.NewTradingSession(sessionConfig)
}

//=============================================================================
