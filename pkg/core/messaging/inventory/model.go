//=============================================================================
//===
//=== Copyright (C) 2023-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package inventory

import (
	"github.com/algotiqa/data-collector/pkg/db"
)

//=============================================================================
//===
//=== General entities
//===
//=============================================================================

type Currency struct {
	Id   uint   `json:"id"`
	Code string `json:"code"`
	Name string `json:"name"`
}

//=============================================================================

type Connection struct {
	Id                   uint   `json:"id"`
	Username             string `json:"username"`
	Code                 string `json:"code"`
	Name                 string `json:"name"`
	SystemCode           string `json:"systemCode"`
	SystemName           string `json:"systemName"`
	Connected            bool   `json:"connected"`
	SupportsData         bool   `json:"supportsData"`
	SupportsMultipleData bool   `json:"supportsMultipleData"`
	SupportsInventory    bool   `json:"supportsInventory"`
}

//=============================================================================

type Exchange struct {
	Id         uint   `json:"id"`
	CurrencyId uint   `json:"currencyId"`
	Code       string `json:"code"`
	Name       string `json:"name"`
	Timezone   string `json:"timezone"`
}

//=============================================================================

type TradingSession struct {
	Id       uint   `json:"id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	Session  string `json:"session"`
}

//=============================================================================
//===
//=== Data product
//===
//=============================================================================

type DataProduct struct {
	Id              uint             `json:"id"`
	ConnectionId    uint             `json:"connectionId"`
	ExchangeId      uint             `json:"exchangeId"`
	Username        string           `json:"username"`
	Symbol          string           `json:"symbol"`
	Name            string           `json:"name"`
	MarketType      string           `json:"marketType"`
	ProductType     string           `json:"productType"`
	Months          string           `json:"months"`
	RolloverTrigger db.DPRollTrigger `json:"rolloverTrigger"`
	SessionId       uint             `json:"sessionId"`
}

//=============================================================================

type DataProductMessage struct {
	DataProduct    DataProduct    `json:"dataProduct"`
	Connection     Connection     `json:"connection"`
	Exchange       Exchange       `json:"exchange"`
	TradingSession TradingSession `json:"tradingSession"`
}

//=============================================================================
//===
//=== Broker product
//===
//=============================================================================

type BrokerProduct struct {
	Id               uint    `json:"id"`
	Username         string  `json:"username"`
	Symbol           string  `json:"symbol"`
	Name             string  `json:"name"`
	PointValue       float64 `json:"pointValue"`
	CostPerOperation float64 `json:"costPerOperation"`
}

//=============================================================================

type BrokerProductMessage struct {
	BrokerProduct BrokerProduct `json:"brokerProduct"`
	Connection    Connection    `json:"connection"`
	Exchange      Exchange      `json:"exchange"`
	Currency      Currency      `json:"currency"`
}

//=============================================================================
