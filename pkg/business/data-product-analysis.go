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
	"github.com/algotiqa/core/datatype"
	"github.com/algotiqa/data-collector/pkg/core"
	"github.com/algotiqa/data-collector/pkg/ds"
	"golang.org/x/exp/stats"
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
	SqnLen = 100
	AtrLen = 20
)

//=============================================================================

type DataProductAnalysisSpec struct {
	Id       uint
	BackDays int
	Config   *DataConfig
}

//=============================================================================

type DataProductAnalysisResponse struct {
	Id           uint             `json:"id"`
	Symbol       string           `json:"symbol"`
	From         datatype.IntDate `json:"from"`
	To           datatype.IntDate `json:"to"`
	Days         int              `json:"days"`
	DailyResults []*DailyResult   `json:"dailyResults"`
}

//=============================================================================

type DailyResult struct {
	Date            datatype.IntDate `json:"date"`
	Price           float64          `json:"price"`
	PercDailyChange float64          `json:"percDailyChange"`
	Sqn100          float64          `json:"sqn100"`
	TrueRange       float64          `json:"trueRange"`
	PercAtr20       float64          `json:"percAtr20"`
	Direction       int              `json:"direction"`
	Volatility      int              `json:"volatility"`
}

//=============================================================================

func AnalyzeProduct(c *auth.Context, spec *DataProductAnalysisSpec) (*DataProductAnalysisResponse, error) {
	spec.Config.DataConfig.Timeframe = "1440m"

	//--- Save symbol as it is changed by getDataPoints to loop over the instruments
	symbol := spec.Config.DataConfig.Symbol
	params := parseProductDataParams(spec)

	dataPoints, err := getDataPoints(params, spec.Config)
	if err != nil {
		return nil, err
	}

	initialResults := createDailyResults(dataPoints)
	dailyResults := calcSqnAndAtr(initialResults)

	res := &DataProductAnalysisResponse{
		Id:           spec.Id,
		Symbol:       symbol,
		From:         datatype.ToIntDate(&params.From),
		To:           datatype.ToIntDate(&params.To),
		Days:         len(dailyResults),
		DailyResults: dailyResults,
	}

	calcAllVolatility(res)
	normalizeValues(res)

	return res, nil
}

//=============================================================================
//===
//=== Private functions
//===
//=============================================================================

func parseProductDataParams(spec *DataProductAnalysisSpec) *DataInstrumentDataParams {
	to := time.Now()
	from := to.Add(-time.Hour * 24 * time.Duration(spec.BackDays))
	da := ds.NewDataAggregator(nil, time.UTC)

	if spec.BackDays == 0 {
		from = DefaultFrom
	}

	return &DataInstrumentDataParams{
		Location:   time.UTC,
		From:       from.UTC(),
		To:         to.UTC(),
		Reduction:  0,
		Aggregator: da,
	}
}

//=============================================================================

func createDailyResults(dataPoints []*ds.DataPoint) []*DailyResult {
	if len(dataPoints) == 0 {
		return nil
	}

	var results []*DailyResult

	for i, dp := range dataPoints {
		if i > 0 {
			prevClose := dataPoints[i-1].Close
			delta := dp.Close - prevClose

			if prevClose != 0 {
				delta = delta / prevClose
			} else {
				delta = 0
			}

			tr := calcTrueRange(dp, dataPoints[i-1])

			dr := &DailyResult{
				Date:            datatype.ToIntDate(&dp.Time),
				Price:           dp.Close,
				PercDailyChange: delta,
				TrueRange:       tr,
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
			dr.PercAtr20 = calcAtr(list, i-AtrLen+1, i)
			dr.Direction = calcDirection(dr.Sqn100)

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
		sum += list[i].PercDailyChange
	}

	mean := sum / float64(SqnLen)

	//--- Calc stdDev

	sum = 0.0
	diff := 0.0

	for i := start; i <= end; i++ {
		diff = list[i].PercDailyChange - mean
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

	mean := sum / float64(AtrLen)
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

func calcAllVolatility(res *DataProductAnalysisResponse) {
	if res.DailyResults == nil {
		return
	}

	atr20 := flattenAtr(res.DailyResults)
	mean, std := stats.MeanAndStdDev(atr20)

	for i, v := range atr20 {
		res.DailyResults[i].Volatility = calcVolatility(v, mean, std)
	}
}

//=============================================================================

func flattenAtr(list []*DailyResult) []float64 {
	var results []float64

	for _, dr := range list {
		results = append(results, dr.PercAtr20)
	}

	return results
}

//=============================================================================

func calcVolatility(percAtr float64, mean float64, std float64) int {
	if percAtr < mean-std/2 {
		return VolatilityQuiet
	}
	if percAtr < mean+std/2 {
		return VolatilityNormal
	}
	if percAtr < mean+std*3 {
		return VolatilityVolatile
	}

	return VolatilityVeryVolatile
}

//=============================================================================

func normalizeValues(res *DataProductAnalysisResponse) {
	for _, dr := range res.DailyResults {
		dr.PercDailyChange = core.Trunc2d(dr.PercDailyChange * 100)
		dr.PercAtr20 = core.Trunc2d(dr.PercAtr20 * 100)
		dr.Sqn100 = core.Trunc2d(dr.Sqn100)
	}
}

//=============================================================================
