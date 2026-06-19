//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
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

func GetBiasAnalyses(tx *gorm.DB, filter map[string]any, offset int, limit int) (*[]BiasAnalysisFull, error) {
	var list []BiasAnalysisFull
	res := tx.Where(filter).Offset(offset).Limit(limit).Find(&list)

	if res.Error != nil {
		return nil, req.NewServerErrorByError(res.Error)
	}

	return &list, nil
}

//=============================================================================

func GetBiasAnalysesFull(tx *gorm.DB, filter map[string]any, offset int, limit int) (*[]BiasAnalysisFull, error) {
	var list []BiasAnalysisFull

	res := tx.Model(&BiasAnalysis{}).Select("bias_analysis.*, " +
		"data_instrument.symbol as data_symbol, data_instrument.name as data_name," +
		"broker_product.symbol as broker_symbol, broker_product.name as broker_name").
		Joins("LEFT JOIN data_instrument ON bias_analysis.data_instrument_id = data_instrument.id").
		Joins("LEFT JOIN broker_product  ON bias_analysis.broker_product_id  = broker_product.id").
		Where(filter).Offset(offset).Limit(limit).Find(&list)

	if res.Error != nil {
		return nil, req.NewServerErrorByError(res.Error)
	}

	return &list, nil
}

//=============================================================================

func GetBiasAnalysisById(tx *gorm.DB, id uint) (*BiasAnalysis, error) {
	var list []BiasAnalysis
	res := tx.Find(&list, id)

	if res.Error != nil {
		return nil, req.NewServerErrorByError(res.Error)
	}

	if len(list) == 1 {
		return &list[0], nil
	}

	return nil, nil
}

//=============================================================================

func AddBiasAnalysis(tx *gorm.DB, ba *BiasAnalysis) error {
	return tx.Create(ba).Error
}

//=============================================================================

func UpdateBiasAnalysis(tx *gorm.DB, ba *BiasAnalysis) error {
	return tx.Save(ba).Error
}

//=============================================================================

func DeleteBiasAnalysis(tx *gorm.DB, id uint) error {
	return tx.Delete(&BiasAnalysis{}, id).Error
}

//=============================================================================
//=== Bias configs
//=============================================================================

func GetBiasConfigsByAnalysisId(tx *gorm.DB, id uint) (*[]BiasConfig, error) {
	var list []BiasConfig

	filter := map[string]any{}
	filter["bias_analysis_id"] = id

	res := tx.Where(filter).Order("start_day").Find(&list)

	if res.Error != nil {
		return nil, req.NewServerErrorByError(res.Error)
	}

	return &list, nil
}

//=============================================================================

func AddBiasConfig(tx *gorm.DB, bc *BiasConfig) error {
	return tx.Create(bc).Error
}

//=============================================================================

func UpdateBiasConfig(tx *gorm.DB, bc *BiasConfig) error {
	return tx.Save(bc).Error
}

//=============================================================================

func DeleteBiasConfig(tx *gorm.DB, id uint) error {
	return tx.Delete(&BiasConfig{}, id).Error
}

//=============================================================================

func DeleteBiasConfigsByAnalysisId(tx *gorm.DB, id uint) error {
	return tx.Delete(&BiasConfig{}, "bias_analysis_id", id).Error
}

//=============================================================================
