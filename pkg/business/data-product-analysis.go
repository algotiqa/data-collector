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

	"github.com/tradalia/core/auth"
	"github.com/tradalia/core/datatype"
	"github.com/tradalia/data-collector/pkg/ds"
)

//=============================================================================

type DataProductAnalysisSpec struct {
	Id        uint
	BackDays  int
	Config    *DataConfig
}

//=============================================================================

type DataProductAnalysisResponse struct {
	Id           uint
	Symbol       string
	From         datatype.IntDate
	To           datatype.IntDate
	Days         int
	DailyResults []*DailyResult
}

//=============================================================================

const (
	DRDirectionStrongBear = -2
	DRDirectionBear       = -1
	DRDirectionNeutral    =  0
	DRDirectionBull       =  1
	DRDirectionStrongBull =  2
)

const (
	DRVolatilityQuiet        = 0
	DRVolatilityNormal       = 1
	DRVolatilityVolatile     = 2
	DRVolatilityVeryVolatile = 3
)

type DailyResult struct {
	Date            datatype.IntDate
	Price           float64
	PercDailyChange float64
	Sqn100          float64
	TrueRange       float64
	PercAtr20       float64
	Direction       int
	Volatility      int
}

//=============================================================================

const (
	SqnLen = 100
	AtrLen =  20
)

//=============================================================================

func AnalyzeProduct(c *auth.Context, spec *DataProductAnalysisSpec) (*DataProductAnalysisResponse,error){
	spec.Config.DataConfig.Timeframe = "1440m"

	params := parseProductDataParams(spec)

	dataPoints, err := getDataPoints(params, spec.Config)
	if err != nil {
		return nil, err
	}

	res := &DataProductAnalysisResponse{
		Id     : spec.Id,
		Symbol : spec.Config.DataConfig.Symbol,
		From   : datatype.ToIntDate(&params.From),
		To     : datatype.ToIntDate(&params.To),
		Days   : spec.BackDays,
	}

	initialResults := createDailyResults(dataPoints)
	dailyResults   := calcSqnAndAtr(initialResults)

	res.DailyResults = dailyResults

	return res, nil
}

//=============================================================================
//===
//=== Private functions
//===
//=============================================================================

func parseProductDataParams(spec *DataProductAnalysisSpec) *DataInstrumentDataParams {
	to   := time.Now()
	from := to.Add( -time.Hour * 24 * time.Duration(spec.BackDays))
	da   := ds.NewDataAggregator(nil, time.UTC)

	if spec.BackDays == 0 {
		from = DefaultFrom
	}

	return &DataInstrumentDataParams{
		Location  : time.UTC,
		From      : from.UTC(),
		To        : to.UTC(),
		Reduction : 0,
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
			prevClose := dataPoints[i -1].Close
			delta     := dp.Close - prevClose

			if prevClose != 0 {
				delta  = delta  / prevClose
			} else {
				delta  = 0
			}

			tr := calcTrueRange(dp, dataPoints[i -1])

			dr := &DailyResult{
				Date            : datatype.ToIntDate(&dp.Time),
				Price           : dp.Close,
				PercDailyChange : delta,
				TrueRange       : tr,
			}

			results = append(results, dr)
		}
	}

	return results
}

//=============================================================================

func calcSqnAndAtr(list []*DailyResult) []*DailyResult {
	var result []*DailyResult

	for i, dr := range list {
		if i >= SqnLen -1 {
			dr.Sqn100    = calcSqn(list, i - SqnLen + 1, i)
			dr.PercAtr20 = calcAtr(list, i - AtrLen + 1, i)
			result = append(result, dr)
		}
	}

	return result
}

//=============================================================================

func calcSqn(list []*DailyResult, start int, end int) float64 {
	//--- Calc mean

	sum := 0.0

	for i:=start; i<=end; i++ {
		sum += list[i].PercDailyChange
	}

	mean := sum / float64(SqnLen)

	//--- Calc stdDev

	sum   = 0.0
	diff := 0.0

	for i:=start; i<=end; i++ {
		diff = list[i].PercDailyChange - mean
		sum += diff*diff
	}

	stdDev := math.Sqrt(sum/float64(SqnLen))

	return mean * math.Sqrt(SqnLen) / stdDev
}

//=============================================================================

func calcTrueRange(curr *ds.DataPoint, prev *ds.DataPoint) float64 {
	range1 := curr.High - curr.Low
	range2 := math.Abs(curr.High - prev.Close)
	range3 := math.Abs(curr.Low - prev.Close)

	return math.Max(math.Max(range1, range2), range3)
}

//=============================================================================

func calcAtr(list []*DailyResult, start int, end int) float64 {
	sum := 0.0

	for i:=start; i<=end; i++ {
		sum += list[i].TrueRange
	}

	mean  := sum / float64(AtrLen)
	price := list[end].Price

	if price == 0 {
		return 0.0
	}

	return mean / price
}

//=============================================================================
