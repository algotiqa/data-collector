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
	"errors"

	"github.com/algotiqa/core/auth"
	"github.com/algotiqa/data-collector/pkg/db"
	"gorm.io/gorm"
)

//=============================================================================

func GetBiasConfigsByAnalysisId(tx *gorm.DB, c *auth.Context, baId uint) (*[]*BiasConfig, error) {
	list, err := db.GetBiasConfigsByAnalysisId(tx, baId)

	if err != nil {
		c.Log.Error("GetBiasConfigsByAnalysisId: Could not retrieve bias configs", "error", err.Error())
		return nil, err
	}

	//--- DON'T replace with 'var result []*BiasConfig' because a null is returned when no records are found
	result := []*BiasConfig{}

	for _, dbc := range *list {
		bc := &BiasConfig{}
		bc.FromBiasConfig(&dbc)
		result = append(result, bc)
	}

	return &result, nil
}

//=============================================================================

func AddBiasConfig(tx *gorm.DB, c *auth.Context, baId uint, bcs *BiasConfigSpec) (*db.BiasConfig, error) {
	c.Log.Info("AddBiasConfig: Adding a new bias config", "baId", baId)

	if err := checkBiasConfigSpec(c, bcs); err != nil {
		return nil, err
	}

	bc := bcs.ToBiasConfig()
	bc.BiasAnalysisId = baId
	err := db.AddBiasConfig(tx, bc)

	if err != nil {
		c.Log.Error("AddBiasConfig: Could not add a new bias config", "error", err.Error())
		return nil, err
	}

	c.Log.Info("AddBiasConfig: Bias config added", "baId", baId, "id", bc.Id)
	return bc, err
}

//=============================================================================

func UpdateBiasConfig(tx *gorm.DB, c *auth.Context, baId uint, id uint, bcs *BiasConfigSpec) (*db.BiasConfig, error) {
	c.Log.Info("UpdateBiasConfig: Updating a bias config", "id", id, "baId", baId)

	if err := checkBiasConfigSpec(c, bcs); err != nil {
		return nil, err
	}

	bc := bcs.ToBiasConfig()
	bc.Id = id
	bc.BiasAnalysisId = baId

	err := db.UpdateBiasConfig(tx, bc)
	if err != nil {
		c.Log.Error("UpdateBiasConfig: Could not update a bias config", "error", err.Error())
		return nil, err
	}

	c.Log.Info("UpdateBiasConfig: Bias config updated", "id", bc.Id, "baId", bc.BiasAnalysisId)
	return bc, err
}

//=============================================================================

func DeleteBiasConfig(tx *gorm.DB, c *auth.Context, baId uint, id uint) (bool, error) {
	c.Log.Info("DeleteBiasConfig: Deleting a bias config", "id", id, "baId", baId)

	err := db.DeleteBiasConfig(tx, id)
	if err != nil {
		c.Log.Error("DeleteBiasConfig: Could not delete a bias config", "error", err.Error())
		return false, err
	}

	c.Log.Info("DeleteBiasConfig: Bias config deleted", "id", id, "baId", baId)
	return true, err
}

//=============================================================================
//===
//=== Private functions
//===
//=============================================================================

func checkBiasConfigSpec(c *auth.Context, bcs *BiasConfigSpec) error {
	if bcs.Operation != 0 && bcs.Operation != 1 {
		err := errors.New("operation can only be 0 (for long) or 1 (for short)")
		c.Log.Error("checkBiasConfigSpec: Invalid bias config spec", "error", err.Error())
		return err
	}

	return nil
}

//=============================================================================
