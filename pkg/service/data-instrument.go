//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package service

import (
	"github.com/algotiqa/core/auth"
	"github.com/algotiqa/core/dbms"
	"github.com/algotiqa/data-collector/pkg/business"
	"github.com/algotiqa/data-collector/pkg/core"
	"github.com/algotiqa/data-collector/pkg/core/jobmanager"
	"github.com/algotiqa/data-collector/pkg/db"
	"gorm.io/gorm"
)

//=============================================================================

func getDataInstruments(c *auth.Context) {
	err := dbms.RunInTransaction(func(tx *gorm.DB) error {
		list, err := business.GetDataInstruments(tx, c)

		if err != nil {
			return err
		}

		return c.ReturnList(list, 0, len(*list), len(*list))
	})

	c.ReturnError(err)
}

//=============================================================================

func getDataInstrumentById(c *auth.Context) {
	id, err := c.GetIdFromUrl()

	if err == nil {
		var details bool
		details, err = c.GetParamAsBool("details", false)

		if err == nil {
			err = dbms.RunInTransaction(func(tx *gorm.DB) error {
				var di *business.DataInstrumentExt
				di, err = business.GetDataInstrumentById(tx, c, id, details)

				if err != nil {
					return err
				}

				return c.ReturnObject(di)
			})
		}
	}

	c.ReturnError(err)
}

//=============================================================================

func getDataInstrumentData(c *auth.Context) {
	var result *business.DataInstrumentDataResponse
	var config *core.QueryConfig

	id, err := c.GetIdFromUrl()

	if err == nil {
		err = dbms.RunInTransaction(func(tx *gorm.DB) error {
			sessionConfig := c.GetParamAsString("sessionConfig", "")
			cfg, err1 := business.CreateQueryConfig(tx, id, sessionConfig)
			config = cfg
			return err1
		})

		if err == nil {
			spec := createQuerySpec(c, id, config)
			result, err = business.GetDataInstrumentDataById(c, spec)
			if err == nil {
				_ = c.ReturnObject(result)
				return
			}
		}
	}

	c.ReturnError(err)
}

//=============================================================================

func reloadDataInstrumentData(c *auth.Context) {
	id, err := c.GetIdFromUrl()

	if err == nil {
		var job *db.DownloadJob
		var blk *db.DataBlock
		err = dbms.RunInTransaction(func(tx *gorm.DB) error {
			job, blk, err = business.ReloadDataInstrumentData(tx, c, id)
			return err
		})

		if err == nil {
			sj := jobmanager.NewScheduledJob(c.Session.Username, blk, job)
			jobmanager.AddScheduledJob(sj)
		}
	}

	c.ReturnError(err)
}

//=============================================================================

func createQuerySpec(c *auth.Context, id uint, config *core.QueryConfig) *business.QuerySpec {
	return &business.QuerySpec{
		Id       : id,
		From     : c.GetParamAsString("from",      ""),
		To       : c.GetParamAsString("to",        ""),
		DaysBack : c.GetParamAsString("daysBack",  ""),
		Timezone : c.GetParamAsString("timezone",  ""),
		Timeframe: c.GetParamAsString("timeframe", ""),
		Reduction: c.GetParamAsString("reduction", ""),
		Limit    : c.GetParamAsString("limit",     ""),
		Config   : config,
	}
}

//=============================================================================
