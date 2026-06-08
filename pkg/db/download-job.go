//=============================================================================
/*
Copyright © 2025 Andrea Carboni andrea.carboni71@gmail.com

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
