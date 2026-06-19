//=============================================================================
//===
//=== Copyright (C) 2025-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package system

//=============================================================================

type ConnectionStatus int

const (
	ConnectionStatusDisconnected = 0
	ConnectionStatusConnecting   = 1
	ConnectionStatusConnected    = 2
)

//=============================================================================

type ConnectionChangeSystemMessage struct {
	Username       string           `json:"username"`
	ConnectionCode string           `json:"connectionCode"`
	SystemCode     string           `json:"systemCode"`
	Status         ConnectionStatus `json:"status"`
}

//=============================================================================
