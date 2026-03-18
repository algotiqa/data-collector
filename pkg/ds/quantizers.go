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
	"strconv"
	"time"
)

//=============================================================================
//===
//=== Quantizer
//===
//=============================================================================

type Quantizer interface {
	BaseTimeframe() string
	TargetTimeframe() string

	// Quantize maps the dpTime (which is always in the data product's timezone)
	// into a quantized version used for aggregation
	Quantize(dpTime time.Time) time.Time
}

//=============================================================================
//===
//=== Identity quantization
//===
//=============================================================================

type QuantizerIdentity struct {
	baseTimeframe string
}

//=============================================================================

func NewQuantizerIdentity(baseTimeframe int) Quantizer {
	return &QuantizerIdentity{
		baseTimeframe: strconv.Itoa(baseTimeframe) + "m",
	}
}

//=============================================================================

func (f *QuantizerIdentity) BaseTimeframe() string {
	return f.baseTimeframe
}

//=============================================================================

func (f *QuantizerIdentity) TargetTimeframe() string {
	return f.baseTimeframe
}

//=============================================================================

func (f *QuantizerIdentity) Quantize(dpTime time.Time) time.Time {
	return dpTime
}

//=============================================================================
//===
//=== 1m to 5m quantization
//===
//=============================================================================

type Quantizer1mTo5m struct{}

//=============================================================================

func NewQuantizer1mTo5m() Quantizer {
	return &Quantizer1mTo5m{}
}

//=============================================================================

func (f *Quantizer1mTo5m) BaseTimeframe() string {
	return "1m"
}

//=============================================================================

func (f *Quantizer1mTo5m) TargetTimeframe() string {
	return "5m"
}

//=============================================================================

func (f *Quantizer1mTo5m) Quantize(dpTime time.Time) time.Time {
	mins := dpTime.Minute()

	if mins == 0 {
		return dpTime
	}
	if mins <= 5 {
		return dpTime.Add(time.Minute * time.Duration(5-mins))
	}
	if mins <= 10 {
		return dpTime.Add(time.Minute * time.Duration(10-mins))
	}
	if mins <= 15 {
		return dpTime.Add(time.Minute * time.Duration(15-mins))
	}
	if mins <= 20 {
		return dpTime.Add(time.Minute * time.Duration(20-mins))
	}
	if mins <= 25 {
		return dpTime.Add(time.Minute * time.Duration(25-mins))
	}
	if mins <= 30 {
		return dpTime.Add(time.Minute * time.Duration(30-mins))
	}
	if mins <= 35 {
		return dpTime.Add(time.Minute * time.Duration(35-mins))
	}
	if mins <= 40 {
		return dpTime.Add(time.Minute * time.Duration(40-mins))
	}
	if mins <= 45 {
		return dpTime.Add(time.Minute * time.Duration(45-mins))
	}
	if mins <= 50 {
		return dpTime.Add(time.Minute * time.Duration(50-mins))
	}
	if mins <= 55 {
		return dpTime.Add(time.Minute * time.Duration(55-mins))
	}

	return dpTime.Add(time.Minute * time.Duration(60-mins))
}

//=============================================================================
//===
//=== 5m to 10m quantization
//===
//=============================================================================

type Quantizer5mTo10m struct{}

//=============================================================================

func NewQuantizer5mTo10m() Quantizer {
	return &Quantizer5mTo10m{}
}

//=============================================================================

func (f *Quantizer5mTo10m) BaseTimeframe() string {
	return "5m"
}

//=============================================================================

func (f *Quantizer5mTo10m) TargetTimeframe() string {
	return "10m"
}

//=============================================================================

func (f *Quantizer5mTo10m) Quantize(dpTime time.Time) time.Time {
	mins := dpTime.Minute()

	if mins == 0 {
		return dpTime
	}
	if mins <= 10 {
		return dpTime.Add(time.Minute * time.Duration(10-mins))
	}
	if mins <= 20 {
		return dpTime.Add(time.Minute * time.Duration(20-mins))
	}
	if mins <= 30 {
		return dpTime.Add(time.Minute * time.Duration(30-mins))
	}
	if mins <= 40 {
		return dpTime.Add(time.Minute * time.Duration(40-mins))
	}
	if mins <= 50 {
		return dpTime.Add(time.Minute * time.Duration(50-mins))
	}

	return dpTime.Add(time.Minute * time.Duration(60-mins))
}

//=============================================================================
//===
//=== 5m to 15m quantization
//===
//=============================================================================

type Quantizer5mTo15m struct{}

//=============================================================================

func NewQuantizer5mTo15m() Quantizer {
	return &Quantizer5mTo15m{}
}

//=============================================================================

func (f *Quantizer5mTo15m) BaseTimeframe() string {
	return "5m"
}

//=============================================================================

func (f *Quantizer5mTo15m) TargetTimeframe() string {
	return "15m"
}

//=============================================================================

func (f *Quantizer5mTo15m) Quantize(dpTime time.Time) time.Time {
	mins := dpTime.Minute()

	if mins == 0 {
		return dpTime
	}
	if mins <= 15 {
		return dpTime.Add(time.Minute * time.Duration(15-mins))
	}
	if mins <= 30 {
		return dpTime.Add(time.Minute * time.Duration(30-mins))
	}
	if mins <= 45 {
		return dpTime.Add(time.Minute * time.Duration(45-mins))
	}

	return dpTime.Add(time.Minute * time.Duration(60-mins))
}

//=============================================================================
//===
//=== 15m to 30m quantization
//===
//=============================================================================

type Quantizer15mTo30m struct{}

//=============================================================================

func NewQuantizer15mTo30m() Quantizer {
	return &Quantizer15mTo30m{}
}

//=============================================================================

func (f *Quantizer15mTo30m) BaseTimeframe() string {
	return "15m"
}

//=============================================================================

func (f *Quantizer15mTo30m) TargetTimeframe() string {
	return "30m"
}

//=============================================================================

func (f *Quantizer15mTo30m) Quantize(dpTime time.Time) time.Time {
	mins := dpTime.Minute()

	if mins == 0 {
		return dpTime
	}
	if mins <= 30 {
		return dpTime.Add(time.Minute * time.Duration(30-mins))
	}

	return dpTime.Add(time.Minute * time.Duration(60-mins))
}

//=============================================================================
//===
//=== 15m to 60m quantization
//===
//=============================================================================

type Quantizer15mTo60m struct{}

//=============================================================================

func NewQuantizer15mTo60m() Quantizer {
	return &Quantizer15mTo60m{}
}

//=============================================================================

func (f *Quantizer15mTo60m) BaseTimeframe() string {
	return "15m"
}

//=============================================================================

func (f *Quantizer15mTo60m) TargetTimeframe() string {
	return "60m"
}

//=============================================================================

func (f *Quantizer15mTo60m) Quantize(dpTime time.Time) time.Time {
	mins := dpTime.Minute()

	if mins == 0 {
		return dpTime
	}

	return dpTime.Add(time.Minute * time.Duration(60-mins))
}

//=============================================================================
//===
//=== 60m to 1440m quantization
//===
//=============================================================================

type Quantizer60mTo1440m struct{}

//=============================================================================

func NewQuantizer60mTo1440m() Quantizer {
	return &Quantizer60mTo1440m{}
}

//=============================================================================

func (f *Quantizer60mTo1440m) BaseTimeframe() string {
	return "60m"
}

//=============================================================================

func (f *Quantizer60mTo1440m) TargetTimeframe() string {
	return "1440m"
}

//=============================================================================

func (f *Quantizer60mTo1440m) Quantize(dpTime time.Time) time.Time {
	hours, mins, _ := dpTime.Clock()

	if mins == 0 && hours == 0 {
		return dpTime
	}

	return dpTime.Add(time.Minute * time.Duration(1440-mins-hours*60))
}

//=============================================================================
//===
//=== 5m to 1440m quantization
//===
//=============================================================================

type Quantizer5mTo1440m struct{}

//=============================================================================

func NewQuantizer5mTo1440m() Quantizer {
	return &Quantizer5mTo1440m{}
}

//=============================================================================

func (f *Quantizer5mTo1440m) BaseTimeframe() string {
	return "5m"
}

//=============================================================================

func (f *Quantizer5mTo1440m) TargetTimeframe() string {
	return "1440m"
}

//=============================================================================

func (f *Quantizer5mTo1440m) Quantize(dpTime time.Time) time.Time {
	hours, mins, _ := dpTime.Clock()

	if mins == 0 && hours == 0 {
		return dpTime
	}

	return dpTime.Add(time.Minute * time.Duration(1440-mins-hours*60))
}

//=============================================================================
//===
//=== 1m to generic quantization
//===
//=============================================================================

type Quantizer1mToGeneric struct {
	Target int
}

//=============================================================================

func NewQuantizer1mToGeneric(target int) Quantizer {
	return &Quantizer1mToGeneric{
		Target: target,
	}
}

//=============================================================================

func (f *Quantizer1mToGeneric) BaseTimeframe() string {
	return "1m"
}

//=============================================================================

func (f *Quantizer1mToGeneric) TargetTimeframe() string {
	return strconv.Itoa(f.Target) + "m"
}

//=============================================================================

func (f *Quantizer1mToGeneric) Quantize(dpTime time.Time) time.Time {
	//TODO:
	return dpTime
}

//=============================================================================
