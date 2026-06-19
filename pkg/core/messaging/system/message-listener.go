//=============================================================================
//===
//=== Copyright (C) 2025-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package system

import (
	"encoding/json"
	"log/slog"

	"github.com/algotiqa/core/dbms"
	"github.com/algotiqa/core/msg"
	"github.com/algotiqa/data-collector/pkg/core/jobmanager"
	"github.com/algotiqa/data-collector/pkg/db"
	"gorm.io/gorm"
)

//=============================================================================

func HandleMessage(m *msg.Message) bool {

	slog.Info("HandleMessage: New message received", "source", m.Source, "type", m.Type)

	if m.Source == msg.SourceSystem {
		if m.Type == msg.TypeRestart {
			return handleSystemAdapterRestart()
		}
	} else if m.Source == msg.SourceConnection {
		ccm := ConnectionChangeSystemMessage{}
		err := json.Unmarshal(m.Entity, &ccm)
		if err != nil {
			slog.Error("Dropping badly formatted message!", "entity", string(m.Entity))
			return true
		}

		if m.Type == msg.TypeChange {
			return handleConnectionChange(&ccm)
		}
	}

	slog.Error("handleMessage: Dropping message with unknown source/type!", "source", m.Source, "type", m.Type)
	return true
}

//=============================================================================

func handleSystemAdapterRestart() bool {
	slog.Info("handleSystemAdapterRestart: Unsetting connection status flag to all connections")

	err := dbms.RunInTransaction(func(tx *gorm.DB) error {
		return db.DisconnectAll(tx)
	})

	if err != nil {
		slog.Error("handleSystemAdapterRestart: Raised error while disconnecting", "error", err.Error())
	} else {
		slog.Info("handleSystemAdapterRestart: Disconnection complete")
	}

	//--- We have to disconnect regardless of any error above
	jobmanager.DisconnectAll()

	return err == nil
}

//=============================================================================

func handleConnectionChange(ccm *ConnectionChangeSystemMessage) bool {
	if ccm.Status == ConnectionStatusConnecting {
		return true
	}

	slog.Info("handleConnectionChange: Updating connection status", "user", ccm.Username, "connectionCode", ccm.ConnectionCode, "status", ccm.Status)

	connected := ccm.Status == ConnectionStatusConnected
	err := dbms.RunInTransaction(func(tx *gorm.DB) error {
		return db.SetConnectionStatus(tx, ccm.Username, ccm.ConnectionCode, connected)
	})

	if err != nil {
		slog.Info("handleConnectionChange: Raise an error while changing connection status", "user", ccm.Username, "connectionCode", ccm.ConnectionCode, "status", ccm.Status, "error", err.Error())
	} else {
		jobmanager.SetConnection(ccm.SystemCode, ccm.Username, ccm.ConnectionCode, connected)
		slog.Info("handleConnectionChange: Connection status update complete", "user", ccm.Username, "connectionCode", ccm.ConnectionCode, "status", ccm.Status)
	}

	return err == nil
}

//=============================================================================
