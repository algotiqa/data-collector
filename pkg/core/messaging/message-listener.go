//=============================================================================
//===
//=== Copyright (C) 2023-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package messaging

import (
	"log/slog"

	"github.com/algotiqa/core/msg"
	"github.com/algotiqa/data-collector/pkg/core/messaging/collector"
	"github.com/algotiqa/data-collector/pkg/core/messaging/inventory"
	"github.com/algotiqa/data-collector/pkg/core/messaging/system"
)

//=============================================================================

func InitMessageListener() {
	slog.Info("Starting message listeners...")

	go msg.ReceiveMessages(msg.QuInventoryToCollector, inventory.HandleUpdateMessage)
	go msg.ReceiveMessages(msg.QuSystemToCollector,    system.HandleMessage)
	go msg.ReceiveMessages(msg.QuCollectorToInternal,  collector.HandleMessage)
}

//=============================================================================
