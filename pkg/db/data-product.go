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

func GetDataProducts(tx *gorm.DB, filter map[string]any, offset int, limit int) (*[]DataProduct, error) {
	var list []DataProduct
	res := tx.Where(filter).Offset(offset).Limit(limit).Find(&list)

	if res.Error != nil {
		return nil, req.NewServerErrorByError(res.Error)
	}

	return &list, nil
}

//=============================================================================

func GetDataProductById(tx *gorm.DB, id uint) (*DataProduct, error) {
	var list []DataProduct
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

func AddDataProduct(tx *gorm.DB, p *DataProduct) error {
	return tx.Create(p).Error
}

//=============================================================================

func DeleteDataProduct(tx *gorm.DB, id uint) error {
	return tx.Delete(&DataProduct{}, id).Error
}

//=============================================================================

func DisconnectAll(tx *gorm.DB) error {
	return tx.Model(&DataProduct{}).
		Where("supports_multiple_data = false").
		Update("connected", false).Error
}

//=============================================================================

func SetConnectionStatus(tx *gorm.DB, user, code string, flag bool) error {
	return tx.Model(&DataProduct{}).
		Where("username = ? AND connection_code = ?", user, code).
		Update("connected", flag).Error
}

//=============================================================================

func UpdateDataProductFields(tx *gorm.DB, id uint, status DPStatus) error {
	fields := map[string]interface{}{
		"status": status,
	}

	return tx.Model(&DataProduct{}).
		Where("id = ?", id).
		Updates(fields).
		Error
}

//=============================================================================
