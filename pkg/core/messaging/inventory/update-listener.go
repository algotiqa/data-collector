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
	"time"

	"github.com/algotiqa/core/dbms"
	"github.com/algotiqa/core/msg"
	"github.com/algotiqa/data-collector/pkg/core/jobmanager"
	"github.com/algotiqa/data-collector/pkg/db"
	"github.com/algotiqa/data-collector/pkg/ds"
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
		if m.Type == msg.TypeUpdate {
			return true
		}
		if m.Type == msg.TypeDelete {
			return deleteDataProduct(&dpm)
		}
	} else if m.Source == msg.SourceBrokerProduct {
		bpm := BrokerProductMessage{}
		err := json.Unmarshal(m.Entity, &bpm)
		if err != nil {
			slog.Error("HandleUpdateMessage: Dropping badly formatted message (BrokerProduct)!", "entity", string(m.Entity))
			return true
		}

		if m.Type == msg.TypeCreate {
			return setBrokerProduct(&bpm)
		}
		if m.Type == msg.TypeUpdate {
			return setBrokerProduct(&bpm)
		}
		if m.Type == msg.TypeDelete { return deleteBrokerProduct(&bpm) }
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
	err := dbms.RunInTransaction(func(tx *gorm.DB) error {
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

func deleteDataProduct(dpm *DataProductMessage) bool {
	id := dpm.DataProduct.Id
	slog.Info("deleteDataProduct: Data product deletion received", "id", id, "symbol", dpm.DataProduct.Symbol)

	//--- Cancel running jobs and wait for their completion

	cancelRunningJobs(&dpm.DataProduct, &dpm.Connection)

	//--- Retrieve the list of data instruments of the product we want to delete

	list,err := getDataInstrumentsByProductId(id)
	if err != nil {
		slog.Error("deleteDataProduct: Cannot retrieve the list of data instruments", "error", err.Error(), "id", id)
		return false
	}

	//--- First, delete data (it is heavy, so it is done outside a transaction)

	if dpm.Connection.SupportsMultipleData {
		for _, di := range *list {
			config := &ds.DataConfig{
				UserTable: true,
				Selector : id,
				Symbol   : di.Symbol,
			}
			err = ds.DeleteAggregates(config)
			if err != nil {
				slog.Error("deleteDataProduct: Cannot delete aggregates", "error", err.Error(), "id", id)
				return false
			}
		}
	}

	//--- Second, delete records

	err = deleteProductEntities(id, list, dpm.Connection.SupportsMultipleData)
	if err != nil {
		slog.Error("deleteDataProduct: Raised error while deleting data product", "error", err.Error())
	} else {
		slog.Info("deleteDataProduct: Operation complete", "id", id)
	}

	return err == nil
}

//=============================================================================

func cancelRunningJobs(dp *DataProduct, c *Connection) {
	slog.Info("deleteDataProduct: Waiting for all running jobs on data product to cancel", "id", dp.Id, "symbol", dp.Symbol)
	for found := jobmanager.CancelUserJobsOnProduct(dp.Username, c.SystemCode, dp.Symbol); found; {
		time.Sleep(1 * time.Second)
	}
	slog.Info("deleteDataProduct: Running jobs on data product have been successfully cancelled", "id", dp.Id, "symbol", dp.Symbol)
}

//=============================================================================

func deleteProductEntities(id uint, list *[]db.DataInstrument, supportsMultipleData bool) error {
	return dbms.RunInTransaction(func(tx *gorm.DB) error {
		for _, di := range *list {
			err := deleteBiasAnalyses(tx, "data_instrument_id", di.Id)
			if err != nil {
				return err
			}

			err = db.DeleteIngestionJobsByDataInstrumentId(tx, di.Id)
			if err != nil {
				return err
			}

			err = db.DeleteDownloadJobsByDataInstrumentId(tx, di.Id)
			if err != nil {
				return err
			}

			err = db.DeleteDataInstrument(tx, di.Id)
			if err != nil {
				return err
			}

			if supportsMultipleData {
				err = db.DeleteDataBlock(tx, *di.DataBlockId)
				if err != nil {
					return err
				}
			}
		}

		return db.DeleteDataProduct(tx, id)
	})
}

//=============================================================================
//--- If the pointValue or costPerOperation change, it is wise to invalidate the results
//--- of the current bias analyses

func setBrokerProduct(bpm *BrokerProductMessage) bool {
	slog.Info("setBrokerProduct: Broker product change received", "id", bpm.BrokerProduct.Id)

	err := dbms.RunInTransaction(func(tx *gorm.DB) error {
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

func deleteBrokerProduct(bpm *BrokerProductMessage) bool {
	id := bpm.BrokerProduct.Id
	slog.Info("deleteBrokerProduct: Broker product deletion received", "id", id)

	err := dbms.RunInTransaction(func(tx *gorm.DB) error {
		err := deleteBiasAnalyses(tx, "broker_product_id", id)
		if err != nil {
			return err
		}
		return db.DeleteBrokerProduct(tx, id)
	})

	if err != nil {
		slog.Error("deleteBrokerProduct: Raised error while deleting broker product", "error", err.Error())
	} else {
		slog.Info("deleteBrokerProduct: Operation complete", "id", id)
	}

	return err == nil
}

//=============================================================================

func getDataInstrumentsByProductId(id uint) (*[]db.DataInstrument, error) {
	var list *[]db.DataInstrument

	err := dbms.RunInTransaction(func(tx *gorm.DB) error {
		var err error
		list, err = db.GetDataInstrumentsByProductId(tx, id)
		return err
	})

	return list,err
}

//=============================================================================

func deleteBiasAnalyses(tx *gorm.DB, field string, id uint) error {
	filter := map[string]any{}
	filter[field] = id

	list,err := db.GetBiasAnalyses(tx, filter, 0, 5000)
	if err != nil {
		return err
	}

	for _, ba := range *list {
		err = db.DeleteBiasConfigsByAnalysisId(tx, ba.Id)
		if err != nil {
			return err
		}

		err = db.DeleteBiasAnalysis(tx, ba.Id)
		if err != nil {
			return err
		}
	}

	return nil
}

//=============================================================================
