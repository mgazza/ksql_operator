package ksqlparser

import (
	"strconv"
	"strings"
)

type WindowType string

const (
	WindowTypeTumbling = "TUMBLING"
	WindowTypeHopping  = "HOPPING"
	WindowTypeSession  = "SESSION"

	WindowFieldSize        = "SIZE"
	WindowFieldAdvanceBy   = "ADVANCE BY"
	WindowFieldRetention   = "RETENTION"
	WindowFieldGracePeriod = "GRACE PERIOD"

	WindowTimePeriodSecond = "SECOND"
	WindowTimePeriodMinute = "MINUTE"
	WindowTimePeriodHour   = "HOUR"

	WindowTimePeriodSeconds = "SECONDS"
	WindowTimePeriodMinutes = "MINUTES"
	WindowTimePeriodHours   = "HOURS"
)

var windowTimePeriods = []string{
	WindowTimePeriodSeconds,
	WindowTimePeriodMinutes,
	WindowTimePeriodHours,
	WindowTimePeriodSecond,
	WindowTimePeriodMinute,
	WindowTimePeriodHour,
}

type WindowExpression struct {
	Type            string
	Size            int
	SizeType        string
	Advance         int
	AdvanceType     string
	Retention       int
	RetentionType   string
	GracePeriod     int
	GracePeriodType string
}

func (e *WindowExpression) String() string {
	sb := []string{e.Type, ReservedOpenParens}
	if e.Type != WindowTypeSession {
		sb = append(sb, WindowFieldSize)
	}
	sb = append(sb, strconv.Itoa(e.Size), e.SizeType)

	if e.AdvanceType != "" {
		sb = append(sb, ReservedComma, WindowFieldAdvanceBy, strconv.Itoa(e.Advance), e.AdvanceType)
	}
	if e.RetentionType != "" {
		sb = append(sb, ReservedComma, WindowFieldRetention, strconv.Itoa(e.Retention), e.RetentionType)
	}
	if e.GracePeriodType != "" {
		sb = append(sb, ReservedComma, WindowFieldGracePeriod, strconv.Itoa(e.GracePeriod), e.GracePeriodType)
	}
	sb = append(sb, ReservedCloseParens)
	return strings.Join(sb, " ")
}

func (p *parser) parseWindow() (*WindowExpression, error) {
	result := WindowExpression{}
	var err error
	if result.Type, err = p.popOrError(WindowTypeHopping, WindowTypeSession, WindowTypeTumbling); err != nil {
		return nil, err
	}
	//consume the (
	if _, err := p.popOrError(ReservedOpenParens); err != nil {
		return nil, err
	}

	// size

	if i, l := p.peekWithLength(WindowFieldSize); i == WindowFieldSize {
		p.popLength(l)
	}
	result.Size, err = p.parseNumber()
	if err != nil {
		return nil, err
	}
	result.SizeType, err = p.popOrError(windowTimePeriods...)
	if err != nil {
		return nil, err
	}
	t, err := p.popOrError(ReservedCloseParens, ReservedComma)
	if err != nil {
		return nil, err
	}
	if t == ReservedCloseParens {
		// done
		return &result, nil
	}

	for {
		item, err := p.popOrError(WindowFieldAdvanceBy, WindowFieldRetention, WindowFieldGracePeriod)
		if err != nil {
			return nil, err
		}

		// advance
		if strings.ToUpper(item) == WindowFieldAdvanceBy {
			result.Advance, err = p.parseNumber()
			if err != nil {
				return nil, err
			}
			result.AdvanceType, err = p.popOrError(windowTimePeriods...)
			if err != nil {
				return nil, err
			}
			t, err = p.popOrError(ReservedCloseParens, ReservedComma)
			if err != nil {
				return nil, err
			}
			if t == ReservedCloseParens {
				// done
				return &result, nil
			}
			continue
		}

		// retention
		if strings.ToUpper(item) == WindowFieldRetention {
			result.Retention, err = p.parseNumber()
			if err != nil {
				return nil, err
			}
			result.RetentionType, err = p.popOrError(windowTimePeriods...)
			if err != nil {
				return nil, err
			}
			t, err = p.popOrError(ReservedCloseParens, ReservedComma)
			if err != nil {
				return nil, err
			}
			if t == ReservedCloseParens {
				// done
				return &result, nil
			}
			continue
		}

		// grace period
		if strings.ToUpper(item) == WindowFieldGracePeriod {
			result.GracePeriod, err = p.parseNumber()
			if err != nil {
				return nil, err
			}
			result.GracePeriodType, err = p.popOrError(windowTimePeriods...)
			if err != nil {
				return nil, err
			}
			t, err = p.popOrError(ReservedCloseParens)
			if err != nil {
				return nil, err
			}
			return &result, nil
			continue

		}
	}
}
