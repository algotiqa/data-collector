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

func GetBrokerProductById(tx *gorm.DB, id uint) (*BrokerProduct, error) {
	var list []BrokerProduct
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

func AddBrokerProduct(tx *gorm.DB, p *BrokerProduct) error {
	return tx.Create(p).Error
}

//=============================================================================

func UpdateBrokerProduct(tx *gorm.DB, p *BrokerProduct) error {
	return tx.Save(p).Error
}

//=============================================================================

func DeleteBrokerProduct(tx *gorm.DB, id uint) error {
	return tx.Delete(&BrokerProduct{}, id).Error
}

//=============================================================================
