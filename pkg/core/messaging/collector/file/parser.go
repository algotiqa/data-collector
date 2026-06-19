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

	"github.com/algotiqa/types"
)

//=============================================================================

const TradestationCode = "tsa"
const TradestationName = "Tradestation (ASCII)"

//=============================================================================

type DataRange struct {
	FromDay types.Date
	ToDay   types.Date
}

//=============================================================================

type Parser interface {
	Parse(config *ParserContext) error
}

//=============================================================================

func GetParsers() map[string]string {
	var res = map[string]string{}

	res[TradestationCode] = TradestationName

	return res
}

//=============================================================================

func NewParser(code string) (Parser, error) {
	switch code {
	case TradestationCode:
		return &TradestationParser{}, nil
	}

	return nil, errors.New("Unknown parser type : " + code)
}

//=============================================================================
