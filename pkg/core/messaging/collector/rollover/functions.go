//=============================================================================
//===
//=== Copyright (C) 2025-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package rollover

import (
	"time"

	"github.com/algotiqa/data-collector/pkg/db"
)

//=============================================================================

func calcRolloverDate(expirDate time.Time, rollTrigger db.DPRollTrigger) time.Time {
	switch rollTrigger {
	case db.DPRollTriggerSD4:
		return calcRolloverDateByDays(expirDate, 4)
	case db.DPRollTriggerSD6:
		return calcRolloverDateByDays(expirDate, 6)
	case db.DPRollTriggerSD30:
		return calcRolloverDateByDays(expirDate, 30)

	case db.DPRollTriggerBD3:
		return calcRolloverDateByBusinessDays(expirDate, 3)
	}

	panic("invalid rollover trigger")
}

//=============================================================================

func calcRolloverDateByDays(expirDate time.Time, days int) time.Time {
	return expirDate.AddDate(0, 0, -days)
}

//=============================================================================

func calcRolloverDateByBusinessDays(expirDate time.Time, days int) time.Time {
	d := expirDate.AddDate(0, 0, -days)

	for {
		if isBusinessDay(d) {
			return d
		}

		d = d.AddDate(0, 0, -1)
	}
}

//=============================================================================

func isBusinessDay(date time.Time) bool {
	wd := date.Weekday()
	if wd == time.Saturday || wd == time.Sunday {
		return false
	}

	return true
}

//=============================================================================
