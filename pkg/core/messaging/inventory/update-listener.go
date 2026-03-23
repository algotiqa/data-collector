//=============================================================================
/*
Copyright © 2024 Andrea Carboni andrea.carboni71@gmail.com

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
//=============================================================================

package inventory

import (
	"encoding/json"
	"log/slog"

	"github.com/algotiqa/core/msg"
	"github.com/algotiqa/data-collector/pkg/core/jobmanager"
	"github.com/algotiqa/data-collector/pkg/db"
	"gorm.io/gorm"
)

//=============================================================================

func HandleUpdateMessage(m *msg.Message) bool {

	slog.Info("HandleUpdateMessage: New message received", "source", m.Source, "type", m.Type)

	if m.Source == msg.SourceDataProduct {
		dpm := DataProductMessage{}
		err := json.Unmarshal(m.Entity, &dpm)
		if err != nil {
			slog.Error("HandleUpdateMessage: Dropping badly data formatted message (DataProduct)!", "entity", string(m.Entity))
			return true
		}

		if m.Type == msg.TypeCreate {
			return addDataProduct(&dpm)
		}

	} else if m.Source == msg.SourceBrokerProduct {
		bpm := BrokerProductMessage{}
		err := json.Unmarshal(m.Entity, &bpm)
		if err != nil {
			slog.Error("HandleUpdateMessage: Dropping badly formatted message (BrokerProduct)!", "entity", string(m.Entity))
			return true
		}

		if m.Type == msg.TypeCreate || m.Type == msg.TypeUpdate {
			return setBrokerProduct(&bpm)
		}
	} else if m.Source == msg.SourceTradingSystem {
		//--- We don't care
		return true
	}

	slog.Error("HandleUpdateMessage: Dropping message with unknown source/type!", "source", m.Source, "type", m.Type)
	return true
}

//=============================================================================

func addDataProduct(dpm *DataProductMessage) bool {
	slog.Info("addDataProduct: Data product change received", "id", dpm.DataProduct.Id)

	dp := &db.DataProduct{}
	err := db.RunInTransaction(func(tx *gorm.DB) error {
		dp.Id                   = dpm.DataProduct.Id
		dp.Symbol               = dpm.DataProduct.Symbol
		dp.Username             = dpm.DataProduct.Username
		dp.SystemCode           = dpm.Connection.SystemCode
		dp.ConnectionCode       = dpm.Connection.Code
		dp.Connected            = dpm.Connection.Connected
		dp.SupportsMultipleData = dpm.Connection.SupportsMultipleData
		dp.Timezone             = dpm.Exchange.Timezone
		dp.Months               = dpm.DataProduct.Months
		dp.RolloverTrigger      = dpm.DataProduct.RolloverTrigger
		dp.TradingSessionId     = dpm.TradingSession.Id
		dp.TradingSessionConfig = dpm.TradingSession.Session
		dp.Status               = db.DPStatusReady

		if !dp.SupportsMultipleData {
			dp.Status = db.DPStatusFetchingInventory
		}

		return db.AddDataProduct(tx, dp)
	})

	if err != nil {
		slog.Error("Raised error while processing message")
	} else {
		if !dp.SupportsMultipleData {
			jobmanager.SetConnection(dp.SystemCode, dp.Username, dp.ConnectionCode, dp.Connected)
		}

		slog.Info("addDataProduct: Operation complete")
	}

	return err == nil
}

//=============================================================================
//--- If the pointValue or costPerOperation change, it is wise to invalidate the results
//--- of the current bias analyses

func setBrokerProduct(bpm *BrokerProductMessage) bool {
	slog.Info("setBrokerProduct: Broker product change received", "id", bpm.BrokerProduct.Id)

	err := db.RunInTransaction(func(tx *gorm.DB) error {
		bp := &db.BrokerProduct{}

		bp.Id = bpm.BrokerProduct.Id
		bp.Username = bpm.BrokerProduct.Username
		bp.ConnectionCode = bpm.Connection.Code
		bp.Symbol = bpm.BrokerProduct.Symbol
		bp.Name = bpm.BrokerProduct.Name
		bp.PointValue = bpm.BrokerProduct.PointValue
		bp.CostPerOperation = bpm.BrokerProduct.CostPerOperation
		bp.CurrencyCode = bpm.Currency.Code

		return db.UpdateBrokerProduct(tx, bp)
	})

	if err != nil {
		slog.Error("Raised error while processing message")
	} else {
		slog.Info("setBrokerProduct: Operation complete")
	}

	return err == nil
}

//=============================================================================
