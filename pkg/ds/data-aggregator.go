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
	"time"

	"github.com/algotiqa/core/datatype"
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
}

//=============================================================================
//===
//=== SimpleAggregator
//===
//=============================================================================

type SimpleAggregator struct {
	currDp     *DataPoint
	dataPoints []*DataPoint
	quantizer  Quantizer
}

//=============================================================================

func NewSimpleAggregator(q Quantizer) *SimpleAggregator {
	da := &SimpleAggregator{}
	da.dataPoints = []*DataPoint{}
	da.quantizer = q

	return da
}

//=============================================================================
//===
//=== Public methods
//===
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

func (a *SimpleAggregator) Flush() {
	if a.currDp != nil {
		a.dataPoints = append(a.dataPoints, a.currDp)
		a.currDp = nil
	}
}

//=============================================================================

func (a *SimpleAggregator) DataPoints() []*DataPoint {
	return a.dataPoints
}

//=============================================================================

func (a *SimpleAggregator) Aggregate(daDes DataAggregator) {
	for _, dp := range a.DataPoints() {
		daDes.Add(dp)
	}

	daDes.Flush()
}

//=============================================================================

func (a *SimpleAggregator) Clear() {
	a.currDp = nil
	a.dataPoints = []*DataPoint{}
}

//=============================================================================
//===
//=== DailyAggregator
//===
//=============================================================================

type DailyAggregator struct {
	sessionStart datatype.IntTime
	currDp       *DataPoint
	dataPoints   []*DataPoint
}

//=============================================================================

func NewDailyAggregator(sessionStart datatype.IntTime) *DailyAggregator {
	return &DailyAggregator{
		sessionStart: sessionStart,
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
	//--- Aggregation required

	dpTime := dp.Time

	if a.currDp == nil {
		a.currDp = newDataPoint(dp, dpTime)
	} else {
		if a.sessionIsCrossed(dpTime) {
			a.dataPoints = append(a.dataPoints, a.currDp)
			a.currDp = newDataPoint(dp, dpTime)
		} else {
			merge(a.currDp, dp)
			a.currDp.Time = dpTime
		}
	}
}

//=============================================================================

func (a *DailyAggregator) Flush() {
	if a.currDp != nil {
		a.dataPoints = append(a.dataPoints, a.currDp)
		a.currDp = nil
	}
}

//=============================================================================

func (a *DailyAggregator) Clear() {}

//=============================================================================

func (a *DailyAggregator) Aggregate(daDes DataAggregator) {}

//=============================================================================

func (a *DailyAggregator) DataPoints() []*DataPoint {
	return a.dataPoints
}

//=============================================================================

func (a *DailyAggregator) sessionIsCrossed(dpTime time.Time) bool {
	currTime := a.currDp.Time.Hour()*60 + a.currDp.Time.Minute()
	sessTime := (a.sessionStart/100)*60 + (a.sessionStart % 100)

	sesDelta := int(sessTime) - currTime
	newDelta := int(dpTime.Sub(a.currDp.Time).Minutes())

	//--- Exit if currTime >= sessTime
	if sesDelta < 0 {
		return false
	}

	//--- Ok, currTime < sessTime. Now, if sessTime < newTime there is a cross
	return sesDelta < newDelta
}

//=============================================================================
//===
//=== Private functions
//===
//=============================================================================

func merge(cp, dp *DataPoint) {
	cp.High = math.Max(cp.High, dp.High)
	cp.Low = math.Min(cp.Low, dp.Low)
	cp.Close = dp.Close
	cp.UpVolume += dp.UpVolume
	cp.DownVolume += dp.DownVolume
	cp.UpTicks += dp.UpTicks
	cp.DownTicks += dp.DownTicks
	cp.OpenInterest += dp.OpenInterest
}

//=============================================================================

func newDataPoint(dp *DataPoint, t time.Time) *DataPoint {
	return &DataPoint{
		Time:         t,
		Open:         dp.Open,
		High:         dp.High,
		Low:          dp.Low,
		Close:        dp.Close,
		UpVolume:     dp.UpVolume,
		DownVolume:   dp.DownVolume,
		UpTicks:      dp.UpTicks,
		DownTicks:    dp.DownTicks,
		OpenInterest: dp.OpenInterest,
	}
}

//=============================================================================
