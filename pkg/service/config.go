//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package service

import (
	"github.com/algotiqa/core/auth"
	"github.com/algotiqa/data-collector/pkg/core/messaging/collector/file"
)

//=============================================================================

func getParsers(c *auth.Context) {
	res := file.GetParsers()
	_ = c.ReturnObject(res)
}

//=============================================================================
