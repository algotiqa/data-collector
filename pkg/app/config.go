//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package app

import (
	"github.com/algotiqa/core"
)

//=============================================================================

type Datastore struct {
	Address  string
	Name     string
	Username string
	Password string
	Staging  string
}

//=============================================================================

type Config struct {
	core.Application
	core.Database
	core.Authentication
	core.Platform
	core.Messaging
	Datastore
}

//=============================================================================
