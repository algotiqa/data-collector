//=============================================================================
/*
Copyright © 2024 Andrea Carboni andrea.carboni71@gmail.com

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

package service

import (
	"github.com/algotiqa/core/auth"
	"github.com/algotiqa/data-collector/pkg/business"
	"github.com/algotiqa/data-collector/pkg/core"
	"github.com/algotiqa/data-collector/pkg/core/jobmanager"
	"github.com/algotiqa/data-collector/pkg/db"
	"gorm.io/gorm"
)

//=============================================================================

func getDataInstruments(c *auth.Context) {
	err := db.RunInTransaction(func(tx *gorm.DB) error {
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
			err = db.RunInTransaction(func(tx *gorm.DB) error {
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
		err = db.RunInTransaction(func(tx *gorm.DB) error {
			sessionId := c.GetParamAsString("sessionId", "")
			cfg, err1 := business.CreateQueryConfig(tx, id, sessionId)
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
		err = db.RunInTransaction(func(tx *gorm.DB) error {
			job, blk, err = business.ReloadDataInstrumentData(tx, c, id)
			return err
		})

		if err == nil {
			sj := jobmanager.NewScheduledJob(blk, job)
			jobmanager.AddScheduledJob(sj)
		}
	}

	c.ReturnError(err)
}

//=============================================================================

func createQuerySpec(c *auth.Context, id uint, config *core.QueryConfig) *business.QuerySpec {
	return &business.QuerySpec{
		Id:        id,
		From:      c.GetParamAsString("from", ""),
		To:        c.GetParamAsString("to", ""),
		BackDays:  c.GetParamAsString("backDays", ""),
		Timezone:  c.GetParamAsString("timezone", ""),
		Timeframe: c.GetParamAsString("timeframe", ""),
		Reduction: c.GetParamAsString("reduction", ""),
		Config:    config,
	}
}

//=============================================================================
