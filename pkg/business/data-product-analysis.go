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
	DirectionNeutral    = 0
	DirectionBull       = 1
	DirectionStrongBull = 2
)

const (
	VolatilityQuiet        = 0
	VolatilityNormal       = 1
	VolatilityVolatile     = 2
	VolatilityVeryVolatile = 3
)

const (
	SqnLen    = 100
	Atr10Len  = 10
	Atr100Len = SqnLen
)

//=============================================================================

type DataProductAnalysisResponse struct {
	Id           uint           `json:"id"`
	Symbol       string         `json:"symbol"`
	From         types.Date     `json:"from"`
	To           types.Date     `json:"to"`
	Days         int            `json:"days"`
	DailyResults []*DailyResult `json:"dailyResults"`
}

//=============================================================================

type DailyResult struct {
	Date       time.Time `json:"date"`
	Price      float64   `json:"price"`
	PercChange float64   `json:"percChange"`
	Sqn100     float64   `json:"sqn100"`
	TrueRange  float64   `json:"trueRange"`
	Atr10      float64   `json:"atr10"`
	Atr100     float64   `json:"atr100"`
	AtrRatio   float64   `json:"atrRatio"`
	Direction  int       `json:"direction"`
	Volatility int       `json:"volatility"`
}

//=============================================================================

func AnalyzeProduct(c *auth.Context, spec *QuerySpec) (*DataProductAnalysisResponse, error) {
	params, err := NewQueryParams(spec)
	if err != nil {
		return nil, req.NewBadRequestError(err.Error())
	}

	//--- Save symbol as it is changed by getDataPoints to loop over the instruments
	symbol := spec.Config.DataConfig.Symbol

	dataPoints, err := getDataPoints(params, spec.Config)
	if err != nil {
		return nil, err
	}

	initialResults := createDailyResults(dataPoints)
	dailyResults := calcSqnAndAtr(initialResults)

	res := &DataProductAnalysisResponse{
		Id:           spec.Id,
		Symbol:       symbol,
		From:         types.ToDate(params.From),
		To:           types.ToDate(params.To),
		Days:         len(dailyResults),
		DailyResults: dailyResults,
	}

	normalizeValues(res)

	return res, nil
}

//=============================================================================
//===
//=== Private functions
//===
//=============================================================================

func createDailyResults(dataPoints []*ds.DataPoint) []*DailyResult {
	if len(dataPoints) == 0 {
		return nil
	}

	var results []*DailyResult

	for i, dp := range dataPoints {
		if i >= 100 {
			prevClose := dataPoints[i-100].Close
			ratio := 0.0

			if prevClose != 0 {
				ratio = dp.Close - prevClose/prevClose
			}

			tr := calcTrueRange(dp, dataPoints[i-1])

			dr := &DailyResult{
				Date:       dp.Time,
				Price:      dp.Close,
				PercChange: ratio,
				TrueRange:  tr,
			}

			results = append(results, dr)
		}
	}

	return results
}

//=============================================================================

func calcTrueRange(curr *ds.DataPoint, prev *ds.DataPoint) float64 {
	range1 := curr.High - curr.Low
	range2 := math.Abs(curr.High - prev.Close)
	range3 := math.Abs(curr.Low - prev.Close)

	return math.Max(math.Max(range1, range2), range3)
}

//=============================================================================

func calcSqnAndAtr(list []*DailyResult) []*DailyResult {
	var result []*DailyResult

	for i, dr := range list {
		if i >= SqnLen-1 {
			dr.Sqn100 = calcSqn(list, i-SqnLen+1, i)
			dr.Atr10 = calcAtr(list, i-Atr10Len+1, i)
			dr.Atr100 = calcAtr(list, i-Atr100Len+1, i)

			dr.AtrRatio = 0
			if dr.Atr100 != 0 {
				dr.AtrRatio = dr.Atr10 / dr.Atr100
			}

			dr.Direction = calcDirection(dr.Sqn100)
			dr.Volatility = calcVolatility(dr.AtrRatio)

			result = append(result, dr)
		}
	}

	return result
}

//=============================================================================

func calcSqn(list []*DailyResult, start int, end int) float64 {
	//--- Calc mean

	sum := 0.0

	for i := start; i <= end; i++ {
		sum += list[i].PercChange
	}

	mean := sum / float64(SqnLen)

	//--- Calc stdDev

	sum = 0.0
	diff := 0.0

	for i := start; i <= end; i++ {
		diff = list[i].PercChange - mean
		sum += diff * diff
	}

	stdDev := math.Sqrt(sum / float64(SqnLen))

	return mean * math.Sqrt(SqnLen) / stdDev
}

//=============================================================================

func calcAtr(list []*DailyResult, start int, end int) float64 {
	sum := 0.0

	for i := start; i <= end; i++ {
		sum += list[i].TrueRange
	}

	mean := sum / float64(end-start+1)
	price := list[end].Price

	if price == 0 {
		return 0.0
	}

	return mean / price
}

//=============================================================================

func calcDirection(sqn float64) int {
	if sqn < -0.7 {
		return DirectionStrongBear
	}
	if sqn < 0 {
		return DirectionBear
	}
	if sqn < 0.7 {
		return DirectionNeutral
	}
	if sqn < 1.47 {
		return DirectionBull
	}

	return DirectionStrongBull
}

//=============================================================================

func calcVolatility(atrRatio float64) int {

	if atrRatio < 0.8 {
		return VolatilityQuiet
	}
	if atrRatio < 1.2 {
		return VolatilityNormal
	}
	if atrRatio < 2 {
		return VolatilityVolatile
	}

	return VolatilityVeryVolatile
}

//=============================================================================

func normalizeValues(res *DataProductAnalysisResponse) {
	for _, dr := range res.DailyResults {
		dr.PercChange = core.Trunc2d(dr.PercChange * 100)
		dr.Sqn100 = core.Trunc2d(dr.Sqn100)
		dr.Atr10 = core.Trunc2d(dr.Atr10)
		dr.Atr100 = core.Trunc2d(dr.Atr100)
		dr.AtrRatio = core.Trunc2d(dr.AtrRatio)
	}
}

//=============================================================================
