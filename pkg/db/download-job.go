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

func GetDownloadJobs(tx *gorm.DB) (*[]DownloadJobFull, error) {
	var list []DownloadJobFull
	res := tx.Model(&DownloadJob{}).Select("download_job.*, username").
		Joins("LEFT JOIN data_instrument ON download_job.data_instrument_id = data_instrument.id").
		Joins("LEFT JOIN data_product    ON data_instrument.data_product_id = data_product.id").
		Find(&list)

	if res.Error != nil {
		return nil, req.NewServerErrorByError(res.Error)
	}

	return &list, nil
}

//=============================================================================

func AddDownloadJob(tx *gorm.DB, job *DownloadJob) error {
	return tx.Create(job).Error
}

//=============================================================================

func UpdateDownloadJob(tx *gorm.DB, job *DownloadJob) error {
	return tx.Save(job).Error
}

//=============================================================================

func DeleteDownloadJob(tx *gorm.DB, id uint) error {
	return tx.Delete(&DownloadJob{}, id).Error
}

//=============================================================================

func DeleteDownloadJobsByDataInstrumentId(tx *gorm.DB, id uint) error {
	err := tx.Delete(&DownloadJob{}, "data_instrument_id", id).Error
	return req.NewServerErrorByError(err)
}

//=============================================================================
