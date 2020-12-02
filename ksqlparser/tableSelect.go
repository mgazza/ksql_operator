package ksqlparser

import (
	"fmt"
	"strings"
)

type tableSelect struct {
	Expressions aliasedExpressions
	Identifier  identifier
	Window      *WindowExpression
	Where       *[]*Condition
	Group       []*aliasedExpression
	Having      *[]*Condition
}

func (s *tableSelect) String() string {
	var sb []string
	sb = append(sb, s.Expressions.String())
	sb = append(sb, fmt.Sprintf("%s%s", StringOptions.fromPrefix, ReservedFrom), s.Identifier.String())

	if s.Where != nil {
		sb = append(sb, fmt.Sprintf("%s%s", StringOptions.wherePrefix, ReservedWhere))
		for _, c := range *s.Where {
			sb = append(sb, c.String())
		}
	}

	if s.Group != nil {
		sb = append(sb, fmt.Sprintf("%s%s", StringOptions.groupByPrefix, ReservedGroupBy))
		for i, e := range s.Group {
			if i > 0 {
				sb = append(sb, ReservedComma)
			}
			sb = append(sb, e.String())
		}
	}

	if s.Having != nil {
		sb = append(sb, fmt.Sprintf("%s%s", StringOptions.havingPrefix, ReservedHaving))
		for _, c := range *s.Having {
			sb = append(sb, c.String())
		}
	}

	if s.Window != nil {
		sb = append(sb, fmt.Sprintf("%s%s", StringOptions.windowPrefix, ReservedWindow), s.Window.String())
	}

	return strings.Join(sb, " ")
}

func (p *parser) parseTableSelect() (*tableSelect, error) {
	if _, err := p.popOrError(ReservedSelect); err != nil {
		return nil, err
	}

	result := tableSelect{}

	// select expressions
	exprs, err := p.parseExpressionList(ReservedFrom)
	if err != nil {
		return nil, err
	}
	result.Expressions = *exprs

	if _, err := p.popOrError(ReservedFrom); err != nil {
		return nil, err
	}

	i, err := p.parseIdentifierWithAlias()
	if err != nil {
		return nil, err
	}
	result.Identifier = *i

	item, l, err := p.peekWithLengthOrError(ReservedWindow, ReservedWhere, ReservedGroupBy, ReservedHaving, ReservedEmit, ReservedEndOfStatement)
	if err != nil {
		return nil, err
	}
	if item == ReservedWindow {
		p.popLength(l)
		w, err := p.parseWindow()
		if err != nil {
			return nil, err
		}
		result.Window = w
		item, l, err = p.peekWithLengthOrError(ReservedWhere, ReservedGroupBy, ReservedHaving, ReservedEmit, ReservedEndOfStatement)
		if err != nil {
			return nil, err
		}
	}
	if item == ReservedWhere {
		p.popLength(l)
		w, err := p.parseConditions()
		if err != nil {
			return nil, err
		}
		result.Where = &w
		item, l, err = p.peekWithLengthOrError(ReservedGroupBy, ReservedHaving, ReservedEmit, ReservedEndOfStatement)
		if err != nil {
			return nil, err
		}
	}
	if item == ReservedGroupBy {
		p.popLength(l)
		g, err := p.parseExpressionList(ReservedHaving, ReservedEmit, ReservedEndOfStatement)
		if err != nil {
			return nil, err
		}
		result.Group = *g
		item, l, err = p.peekWithLengthOrError(ReservedHaving, ReservedEmit, ReservedEndOfStatement)
		if err != nil {
			return nil, err
		}
	}
	if item == ReservedHaving {
		p.popLength(l)
		h, err := p.parseConditions()
		if err != nil {
			return nil, err
		}
		result.Having = &h
		item, l, err = p.peekWithLengthOrError(ReservedEmit, ReservedEndOfStatement)
	}

	return &result, err
}
