//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package business

import (
	"github.com/algotiqa/core/auth"
	"github.com/algotiqa/core/req"
	"github.com/algotiqa/data-collector/pkg/db"
	"gorm.io/gorm"
)

//=============================================================================

func GetBiasAnalyses(tx *gorm.DB, c *auth.Context, filter map[string]any, offset int, limit int, details bool) (*[]db.BiasAnalysisFull, error) {
	if !c.Session.IsAdmin() {
		filter["username"] = c.Session.Username
	}

	if details {
		return db.GetBiasAnalysesFull(tx, filter, offset, limit)
	}

	return db.GetBiasAnalyses(tx, filter, offset, limit)
}

//=============================================================================

func GetBiasAnalysisById(tx *gorm.DB, c *auth.Context, id uint, details bool) (*BiasAnalysisExt, error) {
	c.Log.Info("GetBiasAnalysisById: Getting a bias analysis", "id", id)

	ba, err := getBiasAnalysisAndCheckAccess(tx, c, id, "GetBiasAnalysisById")
	if err != nil {
		return nil, err
	}

	//--- Get data instrument

	di, err := db.GetDataInstrumentById(tx, ba.DataInstrumentId)
	if err != nil {
		c.Log.Error("GetBiasAnalysisById: Could not retrieve data instrument", "error", err.Error())
		return nil, err
	}

	//--- Get broker product

	bp, err := db.GetBrokerProductById(tx, ba.BrokerProductId)
	if err != nil {
		c.Log.Error("GetBiasAnalysisById: Could not retrieve broker product", "error", err.Error())
		return nil, err
	}

	//--- Add instruments, if it is the case

	var configs *[]*BiasConfig

	if details {
		configs, err = GetBiasConfigsByAnalysisId(tx, c, ba.Id)
	}

	//--- Put all together

	bae := BiasAnalysisExt{*ba, *di, *bp, configs}

	return &bae, nil
}

//=============================================================================

func AddBiasAnalysis(tx *gorm.DB, c *auth.Context, bas *BiasAnalysisSpec) (*db.BiasAnalysis, error) {
	c.Log.Info("AddBiasAnalysis: Adding a new bias analysis", "name", bas.Name)

	var ba db.BiasAnalysis
	ba.Username = c.Session.Username
	ba.DataInstrumentId = bas.DataInstrumentId
	ba.BrokerProductId = bas.BrokerProductId
	ba.Name = bas.Name
	ba.Notes = bas.Notes

	err := db.AddBiasAnalysis(tx, &ba)

	if err != nil {
		c.Log.Error("AddBiasAnalysis: Could not add a new bias analysis", "error", err.Error())
		return nil, err
	}

	c.Log.Info("AddBiasAnalysis: Bias analysis added", "name", ba.Name, "id", ba.Id)
	return &ba, err
}

//=============================================================================

func UpdateBiasAnalysis(tx *gorm.DB, c *auth.Context, id uint, bas *BiasAnalysisSpec) (*db.BiasAnalysis, error) {
	c.Log.Info("UpdateBiasAnalysis: Updating a bias analysis", "id", id, "name", bas.Name)

	ba, err := getBiasAnalysisAndCheckAccess(tx, c, id, "UpdateBiasAnalysis")
	if err != nil {
		return nil, err
	}

	ba.DataInstrumentId = bas.DataInstrumentId
	ba.BrokerProductId = bas.BrokerProductId
	ba.Name = bas.Name
	ba.Notes = bas.Notes

	err = db.UpdateBiasAnalysis(tx, ba)
	if err != nil {
		return nil, err
	}

	c.Log.Info("UpdateBiasAnalysis: Bias analysis", "id", ba.Id, "name", ba.Name)
	return ba, err
}

//=============================================================================

func DeleteBiasAnalysis(tx *gorm.DB, c *auth.Context, id uint) (*db.BiasAnalysis, error) {
	c.Log.Info("DeleteBiasAnalysis: Deleting a bias analysis", "id", id)

	ba, err := getBiasAnalysisAndCheckAccess(tx, c, id, "DeleteBiasAnalysis")
	if err != nil {
		return nil, err
	}

	err = db.DeleteBiasAnalysis(tx, id)
	if err != nil {
		return nil, err
	}

	return ba, nil
}

//=============================================================================
//===
//=== Private functions
//===
//=============================================================================

func getBiasAnalysisAndCheckAccess(tx *gorm.DB, c *auth.Context, id uint, function string) (*db.BiasAnalysis, error) {
	ba, err := db.GetBiasAnalysisById(tx, id)

	if err != nil {
		c.Log.Error(function+": Could not retrieve bias analysis", "error", err.Error())
		return nil, err
	}

	if ba == nil {
		c.Log.Error(function+": Bias analysis was not found", "id", id)
		return nil, req.NewNotFoundError("Bias analysis was not found: %v", id)
	}

	if !c.Session.IsAdmin() {
		if ba.Username != c.Session.Username {
			c.Log.Error(function+": Bias analysis not owned by user", "id", id)
			return nil, req.NewForbiddenError("Bias analysis is not owned by user: %v", id)
		}
	}

	return ba, nil
}

//=============================================================================
