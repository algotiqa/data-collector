//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package business

import "strconv"

//=============================================================================
//===
//=== ExcludedPeriod
//===
//=============================================================================

type ExcludedPeriod struct {
	Year  int16
	Month int16
}

//=============================================================================

func NewExcludedPeriod(value string) (*ExcludedPeriod, error) {
	if len(value) == 4 {
		y, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}

		return &ExcludedPeriod{
			Year : int16(y),
			Month: 0,
		}, nil
	}

	//--- The period is YYYY-[M]M

	y, err1 := strconv.Atoi(value[0:4])
	m, err2 := strconv.Atoi(value[5:])

	if err1 != nil {
		return nil, err1
	}

	if err2 != nil {
		return nil, err2
	}

	return &ExcludedPeriod{
		Year : int16(y),
		Month: int16(m),
	}, nil
}

//=============================================================================

func (ep * ExcludedPeriod) ShouldBeExcluded(month, year int16) bool {
	if year == ep.Year {
		return (ep.Month == 0) || (ep.Month == month)
	}

	return false
}

//=============================================================================
//===
//=== ExcludedPeriod
//===
//=============================================================================

type ExcludedSet struct {
	periods []*ExcludedPeriod
}

//=============================================================================

func NewExcludedSet(items []string) (*ExcludedSet, error) {
	var periods []*ExcludedPeriod

	for _, item := range items {
		ep, err := NewExcludedPeriod(item)

		if err != nil {
			return nil, err
		}

		periods = append(periods, ep)
	}

	return &ExcludedSet{
		periods: periods,
	}, nil
}

//=============================================================================

func (es *ExcludedSet) ShouldBeExcluded(month, year int16) bool {
	for _, ep := range es.periods {
		if ep.ShouldBeExcluded(month, year) {
			return true
		}
	}

	return false
}

//=============================================================================
