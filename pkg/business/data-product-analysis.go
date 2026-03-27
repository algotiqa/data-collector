//=============================================================================
/*
Copyright © 2025 Andrea Carboni andrea.carboni71@gmail.com

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

package business

import (
	"math"
	"strconv"
	"time"

	"github.com/algotiqa/core/auth"
	"github.com/algotiqa/core/req"
	"github.com/algotiqa/data-collector/pkg/core"
	"github.com/algotiqa/data-collector/pkg/ds"
	"github.com/algotiqa/types"
)

//=============================================================================

const (
	DirectionStrongBear = -2
	DirectionBear       = -1
	DirectionNeutral    =  0
	DirectionBull       =  1
	DirectionStrongBull =  2
)

const (
	VolatilityQuiet        = 0
	VolatilityNormal       = 1
	VolatilityVolatile     = 2
	VolatilityVeryVolatile = 3
)

const (
	SqnLen = 100
)

//=============================================================================

type DataProductAnalysisResponse struct {
	Id           uint          `json:"id"`
	Symbol       string        `json:"symbol"`
	From         types.Date    `json:"from"`
	To           types.Date    `json:"to"`
	Bars         int           `json:"bars"`
	Timeframe    int           `json:"timeframe"`
	AtrLength    int           `json:"atrLength"`
	BarResults   []*BarResult  `json:"barResults"`
}

//=============================================================================

type BarResult struct {
	Time          time.Time `json:"time"`
	Close         float64   `json:"close"`
	BarChangePerc float64   `json:"barChangePerc"`
	TrueRange     float64   `json:"trueRange"`
	Sqn100        float64   `json:"sqn100"`
	Atr           float64   `json:"atr"`
	AtrPerc       float64   `json:"atrPerc"`
	AtrMeanPerc   float64   `json:"atrMeanPerc"`
	AtrStdDevPerc float64   `json:"atrStdDevPerc"`
	Direction     int       `json:"direction"`
	Volatility    int       `json:"volatility"`
}

//=============================================================================

func AnalyzeProduct(c *auth.Context, spec *QuerySpec, atrLen string) (*DataProductAnalysisResponse, error) {
	params, err := NewQueryParams(spec)
	if err != nil {
		return nil, req.NewBadRequestError(err.Error())
	}

	atr,err := parseAtrLen(atrLen)
	if err != nil {
		return nil, req.NewBadRequestError(err.Error())
	}

	//--- Save symbol as it is changed by getDataPoints to loop over the instruments
	symbol := spec.Config.DataConfig.Symbol

	dataPoints, err := getDataPoints(params, spec.Config)
	if err != nil {
		return nil, err
	}

	initialResults := createBarResults(dataPoints, atr)
	barResults     := calcSqnAndAtr(initialResults)

	res := &DataProductAnalysisResponse{
		Id        : spec.Id,
		Symbol    : symbol,
		From      : types.ToDate(params.From),
		To        : types.ToDate(params.To),
		Bars      : len(barResults),
		Timeframe : params.Timeframe,
		AtrLength : atr,
		BarResults: barResults,
	}

	normalizeValues(res)

	return res, nil
}

//=============================================================================
//===
//=== Private functions
//===
//=============================================================================

func parseAtrLen(atrLen string) (int, error) {
	if atrLen == "" {
		return 20,nil
	}

	val,err := strconv.Atoi(atrLen)
	if err != nil {
		return 0, req.NewBadRequestError("parameter 'atrLen' must be a number: "+ atrLen)
	}

	if val < 5 || val > 50 {
		return 0, req.NewBadRequestError("parameter 'atrLen' must be between 5 and 50")
	}

	return val, nil
}

//=============================================================================

func createBarResults(dataPoints []*ds.DataPoint, atrLen int) []*BarResult {
	if len(dataPoints) == 0 {
		return nil
	}

	var results []*BarResult

	for i, dp := range dataPoints {
		if i > 0 {
			tr := calcTrueRange(dp, dataPoints[i-1])
			dr := &BarResult{
				Time         : dp.Time,
				Close        : dp.Close,
				BarChangePerc: 0,
				TrueRange    : tr,
			}

			prevClose := dataPoints[i-1].Close

			if prevClose != 0 {
				dr.BarChangePerc = (dp.Close - prevClose)/prevClose
			}

			results = append(results, dr)
			calcAtr(results, atrLen)
		}
	}

	return results
}

//=============================================================================

func calcTrueRange(curr *ds.DataPoint, prev *ds.DataPoint) float64 {
	range1 := curr.High - curr.Low
	range2 := math.Abs(curr.High - prev.Close)
	range3 := math.Abs(curr.Low  - prev.Close)

	return math.Max(math.Max(range1, range2), range3)
}

//=============================================================================

func calcAtr(list []*BarResult, atrLen int) {
	end  := len(list) -1
	last := list[end]

	if end == 0 {
		last.Atr = last.TrueRange
	} else {
		start := end - atrLen +1
		if start < 0 {
			start = 0
		}

		sum := 0.0

		for i := start; i <= end; i++ {
			sum += list[i].TrueRange
		}

		last.Atr = sum / float64(end-start+1)
	}

	if last.Close != 0 {
		last.AtrPerc = last.Atr / last.Close
	}
}

//=============================================================================

func calcSqnAndAtr(list []*BarResult) []*BarResult {
	var result []*BarResult

	for i, dr := range list {
		if i >= SqnLen-1 {
			dr.Sqn100 = calcSqn(list, i-SqnLen +1, i)

			atrMean, atrDev := calcAtrMeanAndStdDev(list, i-SqnLen +1, i)
			dr.AtrMeanPerc   = atrMean
			dr.AtrStdDevPerc = atrDev
			dr.Direction     = calcDirection(dr.Sqn100)
			dr.Volatility    = calcVolatility(dr.AtrPerc, atrMean, atrDev)

			result = append(result, dr)
		}
	}

	return result
}

//=============================================================================

func calcSqn(list []*BarResult, start int, end int) float64 {
	//--- Calc mean

	sum := 0.0

	for i:=start; i<=end; i++ {
		sum += list[i].BarChangePerc
	}

	mean := sum / float64(SqnLen)

	//--- Calc stdDev

	sum   = 0.0
	diff := 0.0

	for i := start; i <= end; i++ {
		diff = list[i].BarChangePerc - mean
		sum += diff * diff
	}

	stdDev := math.Sqrt(sum / float64(SqnLen))

	return mean * math.Sqrt(SqnLen) / stdDev
}

//=============================================================================

func calcAtrMeanAndStdDev(list []*BarResult, start int, end int) (float64, float64) {
	//--- Calc mean

	sum := 0.0

	for i:=start; i<=end; i++ {
		sum += list[i].AtrPerc
	}

	mean := sum / float64(SqnLen)

	//--- Calc stdDev

	sum   = 0.0
	diff := 0.0

	for i:=start; i<=end; i++ {
		diff = list[i].AtrPerc - mean
		sum += diff*diff
	}

	stdDev := math.Sqrt(sum/float64(SqnLen))

	return mean, stdDev
}

//=============================================================================

func calcDirection(sqn float64) int {
	if sqn < -1.47 {
		return DirectionStrongBear
	}
	if sqn < -0.74 {
		return DirectionBear
	}
	if sqn < 0.74 {
		return DirectionNeutral
	}
	if sqn < 1.47 {
		return DirectionBull
	}

	return DirectionStrongBull
}

//=============================================================================

func calcVolatility(percAtr float64, mean float64, std float64) int {
	if percAtr < mean - std/2 { return VolatilityQuiet    }
	if percAtr < mean + std/2 { return VolatilityNormal   }
	if percAtr < mean + std*3 { return VolatilityVolatile }

	return VolatilityVeryVolatile
}

//=============================================================================

func normalizeValues(res *DataProductAnalysisResponse) {
	for _, dr := range res.BarResults {
		dr.BarChangePerc = core.Trunc2d(dr.BarChangePerc * 100)
		dr.Sqn100        = core.Trunc2d(dr.Sqn100)
		dr.Atr           = core.Trunc4d(dr.Atr)
		dr.AtrPerc       = core.Trunc2d(dr.AtrPerc       * 100)
		dr.AtrMeanPerc   = core.Trunc2d(dr.AtrMeanPerc   * 100)
		dr.AtrStdDevPerc = core.Trunc4d(dr.AtrStdDevPerc * 100)
	}
}

//=============================================================================
