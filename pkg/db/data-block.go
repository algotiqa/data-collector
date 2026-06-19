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

func GetGlobalDataBlocks(tx *gorm.DB) (*[]DataBlock, error) {
	var list []DataBlock

	filter := map[string]any{}
	filter["global"] = true

	res := tx.Where(filter).Find(&list)

	if res.Error != nil {
		return nil, req.NewServerErrorByError(res.Error)
	}

	return &list, nil
}

//=============================================================================

func GetDataBlockById(tx *gorm.DB, id uint) (*DataBlock, error) {
	var list []DataBlock
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

func GetDataProductsByBlockId(tx *gorm.DB, id uint) (*[]uint, error) {
	var list []uint

	filter := map[string]any{}
	filter["data_block_id"] = id

	res := tx.
		Table("data_instrument").
		Select("DISTINCT dp.id").
		Joins("JOIN data_block db ON db.id = data_block_id JOIN data_product dp ON dp.id = data_product_id").
		Where(filter).
		Find(&list)

	if res.Error != nil {
		return nil, req.NewServerErrorByError(res.Error)
	}

	return &list, nil
}

//=============================================================================

func AddDataBlock(tx *gorm.DB, db *DataBlock) error {
	return tx.Create(db).Error
}

//=============================================================================

func UpdateDataBlock(tx *gorm.DB, db *DataBlock) error {
	return tx.Save(db).Error
}

//=============================================================================

func DeleteDataBlock(tx *gorm.DB, id uint) error {
	return tx.Delete(&DataBlock{}, id).Error
}

//=============================================================================
