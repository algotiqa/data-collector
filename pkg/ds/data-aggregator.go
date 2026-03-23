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

package ds

import (
	"math"
	"strconv"
	"time"

	"github.com/algotiqa/types"
)

//=============================================================================
//===
//=== DataAggregator interface
//===
//=============================================================================

type DataAggregator interface {
	BaseTimeframe() string
	TargetTimeframe() string

	// Add adds a datapoint. Time is ALWAYS in DataProduct.Timezone location
	Add(dp *DataPoint)
	Flush()
	Clear()
	Aggregate(daDes DataAggregator)
	DataPoints() []*DataPoint
	ToTimezone(loc *time.Location) DataAggregator
}

//=============================================================================
//===
//=== Abstract aggregator
//===
//=============================================================================

type AbstractAggregator struct {
	currDp     *DataPoint
	dataPoints []*DataPoint
}

//=============================================================================

func (a *AbstractAggregator) BaseTimeframe() string {
	panic("abstract method")
}

//=============================================================================

func (a *AbstractAggregator) TargetTimeframe() string {
	panic("abstract method")
}

//=============================================================================

func (a *AbstractAggregator) Add(dp *DataPoint) {
	panic("abstract method")
}

//=============================================================================

func (a *AbstractAggregator) Flush() {
	if a.currDp != nil {
		a.dataPoints = append(a.dataPoints, a.currDp)
		a.currDp     = nil
	}
}

//=============================================================================

func (a *AbstractAggregator) Clear() {
	a.currDp     = nil
	a.dataPoints = []*DataPoint{}
}

//=============================================================================

func (a *AbstractAggregator) Aggregate(daDes DataAggregator) {
	for _, dp := range a.DataPoints() {
		daDes.Add(dp)
	}

	daDes.Flush()
}

//=============================================================================

func (a *AbstractAggregator) DataPoints() []*DataPoint {
	return a.dataPoints
}

//=============================================================================

func (a *AbstractAggregator) ToTimezone(loc *time.Location) DataAggregator {
	for _, dp := range a.DataPoints() {
		dp.Time = dp.Time.In(loc)
	}

	return a
}

//=============================================================================
//===
//=== SimpleAggregator
//===
//=============================================================================

type SimpleAggregator struct {
	AbstractAggregator
	quantizer  Quantizer
}

//=============================================================================

func NewSimpleAggregator(q Quantizer) *SimpleAggregator {
	return &SimpleAggregator{
		AbstractAggregator: AbstractAggregator{
			dataPoints: []*DataPoint{},
		},
		quantizer: q,
	}
}

//=============================================================================

func (a *SimpleAggregator) BaseTimeframe() string {
	return a.quantizer.BaseTimeframe()
}

//=============================================================================

func (a *SimpleAggregator) TargetTimeframe() string {
	return a.quantizer.TargetTimeframe()
}

//=============================================================================

func (a *SimpleAggregator) Add(dp *DataPoint) {
	//--- Handle the no aggregation case

	if a.quantizer == nil {
		a.dataPoints = append(a.dataPoints, dp)
		return
	}

	//--- Aggregation required

	dpTime := a.quantizer.Quantize(dp.Time)

	if a.currDp == nil {
		a.currDp = newDataPoint(dp, dpTime)
	} else {
		if a.currDp.Time.Equal(dpTime) {
			merge(a.currDp, dp)
		} else {
			a.dataPoints = append(a.dataPoints, a.currDp)
			a.currDp = newDataPoint(dp, dpTime)
		}
	}
}

//=============================================================================
//===
//=== StandardAggregator
//===
//=============================================================================

type StandardAggregator struct {
	AbstractAggregator
	session   *types.TradingSession
	firstTime time.Time
	base      int
	target    int
}

//=============================================================================

func NewStandardAggregator(session *types.TradingSession, baseTimeframe, targetTimeframe int) *StandardAggregator {
	return &StandardAggregator{
		AbstractAggregator: AbstractAggregator{
			dataPoints: []*DataPoint{},
		},
		session: session,
		base   : baseTimeframe,
		target : targetTimeframe,
	}
}

//=============================================================================

func NewIdentityAggregator(timeframe int) *StandardAggregator {
	return NewStandardAggregator(nil, timeframe, timeframe)
}

//=============================================================================

func (a *StandardAggregator) BaseTimeframe() string {
	return strconv.Itoa(a.base) +"m"
}

//=============================================================================

func (a *StandardAggregator) TargetTimeframe() string {
	return strconv.Itoa(a.base) +"m"
}

//=============================================================================

func (a *StandardAggregator) Add(dp *DataPoint) {
	dpTime := dp.Time

	if a.currDp == nil {
		a.currDp    = newDataPoint(dp, dpTime)
		a.firstTime = dpTime
	} else {
		crossSlots := false
		if a.session != nil {
			crossSlots = a.session.CrossSlots(a.currDp.Time, dpTime)
		}

		if crossSlots || a.maxBarReached(dpTime) {
			a.dataPoints = append(a.dataPoints, a.currDp)
			a.currDp     = newDataPoint(dp, dpTime)
			a.firstTime  = dpTime
		} else {
			merge(a.currDp, dp)
			a.currDp.Time = dpTime
		}
	}
}

//=============================================================================

func (a *StandardAggregator) maxBarReached(newTime time.Time) bool {
	currBarSize := int(newTime.Unix() - a.firstTime.Unix())
	maxBarSize  := (a.target - a.base) * 60

	return (currBarSize > maxBarSize) || maxBarSize == 0
}

//=============================================================================
//===
//=== DailyAggregator
//===
//=============================================================================

type DailyAggregator struct {
	AbstractAggregator
	session *types.TradingSession
}

//=============================================================================

func NewDailyAggregator(session *types.TradingSession) *DailyAggregator {
	return &DailyAggregator{
		AbstractAggregator: AbstractAggregator{
			dataPoints: []*DataPoint{},
		},
		session: session,
	}
}

//=============================================================================

func (a *DailyAggregator) BaseTimeframe() string {
	return "5m"
}

//=============================================================================

func (a *DailyAggregator) TargetTimeframe() string {
	return "1440m"
}

//=============================================================================

func (a *DailyAggregator) Add(dp *DataPoint) {
	//--- Aggregation required. Data is 5m bars

	dpTime := dp.Time

	if a.currDp == nil {
		a.currDp = newDataPoint(dp, dpTime)
	} else {
		if a.session.CrossSessions(a.currDp.Time, dpTime) {
			a.dataPoints = append(a.dataPoints, a.currDp)
			a.currDp = newDataPoint(dp, dpTime)
		} else {
			merge(a.currDp, dp)
			a.currDp.Time = dpTime
		}
	}
}

//=============================================================================
//===
//=== Private functions
//===
//=============================================================================

func merge(cp, dp *DataPoint) {
	cp.High          = math.Max(cp.High, dp.High)
	cp.Low           = math.Min(cp.Low, dp.Low)
	cp.Close         = dp.Close
	cp.UpVolume     += dp.UpVolume
	cp.DownVolume   += dp.DownVolume
	cp.UpTicks      += dp.UpTicks
	cp.DownTicks    += dp.DownTicks
	cp.OpenInterest += dp.OpenInterest
}

//=============================================================================

func newDataPoint(dp *DataPoint, t time.Time) *DataPoint {
	return &DataPoint{
		Time        : t,
		Open        : dp.Open,
		High        : dp.High,
		Low         : dp.Low,
		Close       : dp.Close,
		UpVolume    : dp.UpVolume,
		DownVolume  : dp.DownVolume,
		UpTicks     : dp.UpTicks,
		DownTicks   : dp.DownTicks,
		OpenInterest: dp.OpenInterest,
	}
}

//=============================================================================
