//=============================================================================
//===
//=== Copyright (C) 2025-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package db

import (
	"github.com/algotiqa/core/req"
	"gorm.io/gorm"
)

//=============================================================================

func AddIngestionJob(tx *gorm.DB, job *IngestionJob) error {
	return tx.Create(job).Error
}

//=============================================================================

func UpdateIngestionJob(tx *gorm.DB, job *IngestionJob) error {
	return tx.Save(job).Error
}

//=============================================================================

func DeleteIngestionJobsByDataInstrumentId(tx *gorm.DB, id uint) error {
	err := tx.Delete(&IngestionJob{}, "data_instrument_id", id).Error
	return req.NewServerErrorByError(err)
}

//=============================================================================
