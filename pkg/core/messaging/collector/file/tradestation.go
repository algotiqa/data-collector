//=============================================================================
//===
//=== Copyright (C) 2024-present Andrea Carboni
//===
//=== This source code is licensed under the Elastic License 2.0 (ELv2) available at:
//=== https://github.com/algotiqa/docs/blob/main/LICENSE.md
//=== By using this file, you agree to the terms and conditions of that license.
//=============================================================================


package file

import (
	"bufio"
	"errors"
	"strings"
	"time"

	"github.com/algotiqa/data-collector/pkg/ds"
)

//=============================================================================

type TradestationParser struct {
	context     *ParserContext
	headerReady bool
	mapFields   map[string]int
	indexDate   int
	indexTime   int
	indexOpen   int
	indexHigh   int
	indexLow    int
	indexClose  int
	indexUp     int
	indexDown   int
}

//=============================================================================

func (p *TradestationParser) Parse(ctx *ParserContext) error {
	p.context = ctx
	scanner := bufio.NewScanner(ctx.Reader)

	for scanner.Scan() {
		line := scanner.Text()
		if !p.headerReady {
			p.headerReady = true
			if err := p.parseHeader(line); err != nil {
				return err
			}
		} else {
			if err := p.parseLine(line, ctx.FileLocation); err != nil {
				return err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return ctx.Flush()
}

//=============================================================================
//===
//=== Private methods
//===
//=============================================================================

func (p *TradestationParser) parseHeader(line string) error {
	fields := strings.Split(line, ",")
	p.mapFields = map[string]int{}

	for i, field := range fields {
		field = strings.Trim(field, "\"")
		p.mapFields[field] = i
	}

	p.indexDate = p.mapFields[Date]
	p.indexTime = p.mapFields[Time]
	p.indexOpen = p.mapFields[Open]
	p.indexHigh = p.mapFields[High]
	p.indexLow = p.mapFields[Low]
	p.indexClose = p.mapFields[Close]
	p.indexUp = p.mapFields[Up]
	p.indexDown = p.mapFields[Down]

	return p.checkBadHeader()
}

//=============================================================================

func (p *TradestationParser) checkBadHeader() error {
	err := p.checkHeaderField(Date)
	if err == nil {
		err = p.checkHeaderField(Time)
		if err == nil {
			err = p.checkHeaderField(Open)
			if err == nil {
				err = p.checkHeaderField(High)
				if err == nil {
					err = p.checkHeaderField(Low)
					if err == nil {
						err = p.checkHeaderField(Close)
						if err == nil {
							err = p.checkHeaderField(Up)
							if err == nil {
								err = p.checkHeaderField(Down)
							}
						}
					}
				}
			}
		}
	}

	return err
}

//=============================================================================

func (p *TradestationParser) checkHeaderField(field string) error {
	if _, ok := p.mapFields[field]; !ok {
		return errors.New("Missing field from header : " + field)
	}

	return nil
}

//=============================================================================

func (p *TradestationParser) parseLine(line string, loc *time.Location) error {
	values := strings.Split(line, ",")
	dp, err := p.createDataPoint(values, loc)
	if err == nil {
		err = p.context.SaveDataPoint(dp, len(line)+1)
	}

	return err
}

//=============================================================================

func (p *TradestationParser) createDataPoint(values []string, loc *time.Location) (*ds.DataPoint, error) {
	var err error
	var up, down int

	dp := &ds.DataPoint{}

	dp.Time, err = parseTimestamp(values[p.indexDate], values[p.indexTime], loc)
	if err == nil {
		dp.Open, err = parseFloat(values[p.indexOpen], Open)
		if err == nil {
			dp.High, err = parseFloat(values[p.indexHigh], High)
			if err == nil {
				dp.Low, err = parseFloat(values[p.indexLow], Low)
				if err == nil {
					dp.Close, err = parseFloat(values[p.indexClose], Close)
					if err == nil {
						up, err = parseInt(values[p.indexUp], Up)
						if err == nil {
							down, err = parseInt(values[p.indexDown], Down)
							if err == nil {
								dp.UpTicks = up
								dp.DownTicks = down
								dp.Time = dp.Time.In(time.UTC)
							}
						}
					}
				}
			}
		}
	}

	return dp, err
}

//=============================================================================
