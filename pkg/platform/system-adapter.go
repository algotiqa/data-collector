//=============================================================================
//===
//=== Copyright (C) 2025-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package platform

import (
	"github.com/algotiqa/core"
	"github.com/algotiqa/core/auth"
	"github.com/algotiqa/core/req"
	"github.com/algotiqa/types"

	"strconv"
)

//=============================================================================

var platform *core.Platform

//=============================================================================
//===
//=== Init
//===
//=============================================================================

func Init(p *core.Platform) {
	platform = p
}

//=============================================================================
//===
//=== Public methods
//===
//=============================================================================

func GetInstruments(username string, connectionCode string, root string) ([]Instrument, error) {
	var res InstrumentResponse

	token, err := auth.Token()
	if err != nil {
		return nil, err
	}

	client := req.GetDefaultClient()
	url := platform.System + "/v1/connections/" + connectionCode + "/roots/" + root + "/instruments"
	err = req.DoGetOnBehalfOf(client, url, &res, token, username)

	if err != nil {
		return nil, err
	}

	return res.Result, nil
}

//=============================================================================

func GetPriceBars(username string, connectionCode string, symbol string, date types.Date) (*PriceBars, error) {
	var res PriceBars

	token, err := auth.Token()
	if err != nil {
		return nil, err
	}

	client := req.GetDefaultClient()
	url := platform.System + "/v1/connections/" + connectionCode + "/instruments/" + symbol + "/bars?date=" + strconv.Itoa(int(date))
	err = req.DoGetOnBehalfOf(client, url, &res, token, username)

	if err != nil {
		return nil, err
	}

	return &res, nil
}

//=============================================================================
//===
//=== Private methods
//===
//=============================================================================
