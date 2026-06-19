//=============================================================================
//===
//=== Copyright (C) 2026-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package collector

import (
	"encoding/json"
	"log/slog"

	"github.com/algotiqa/core/msg"
	"github.com/algotiqa/data-collector/pkg/core/messaging/collector/file"
	"github.com/algotiqa/data-collector/pkg/core/messaging/collector/rollover"
	"github.com/algotiqa/data-collector/pkg/db"
)

//=============================================================================

func HandleMessage(m *msg.Message) bool {
	slog.Info("handleInternalMessage: New internal message received", "source", m.Source, "type", m.Type)

	if m.Source == msg.SourceUploadJob {
		job := db.IngestionJob{}
		err := json.Unmarshal(m.Entity, &job)
		if err != nil {
			slog.Error("handleInternalMessage: Dropping badly formatted message for upload job!", "entity", string(m.Entity))
			return true
		}

		if m.Type == msg.TypeCreate {
			return file.Upload(&job)
		}
	} else if m.Source == msg.SourceRollRecalcJob {
		job := rollover.RecalcJob{}
		err := json.Unmarshal(m.Entity, &job)
		if err != nil {
			slog.Error("handleInternalMessage: Dropping badly formatted message for rollover recalc job!", "entity", string(m.Entity))
			return true
		}

		if m.Type == msg.TypeCreate {
			return rollover.Recalc(&job)
		}
	}

	slog.Error("handleInternalMessage: Dropping message with unknown source/type!", "source", m.Source, "type", m.Type)
	return true
}

//=============================================================================
