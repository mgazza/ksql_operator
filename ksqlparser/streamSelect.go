package ksqlparser

import (
	"fmt"
	"strings"
)

type streamSelect struct {
	Expressions aliasedExpressions
	Identifier  identifier
	Joins       *[]joinExpression
	Where       *[]*Condition
	Partition   string
}

func (s *streamSelect) String() string {
	var sb []string
	sb = append(sb, s.Expressions.String())
	sb = append(sb, fmt.Sprintf("%s%s", StringOptions.fromPrefix, ReservedFrom), s.Identifier.String())
	if s.Joins != nil {
		for _, j := range *s.Joins {
			sb = append(sb, fmt.Sprintf("%s%s", StringOptions.joinPrefix, j.String()))
		}
	}
	if s.Where != nil {
		sb = append(sb, fmt.Sprintf("%s%s", StringOptions.wherePrefix, ReservedWhere))
		for _, w := range *s.Where {
			sb = append(sb, w.String())
		}
	}
	if s.Partition != "" {
		sb = append(sb, fmt.Sprintf("%s%s", StringOptions.partitionByPrefix, ReservedPartitionBy), s.Partition)
	}

	return strings.Join(sb, " ")
}

func (p *parser) parseStreamSelect() (*streamSelect, error) {
	if _, err := p.popOrError(ReservedSelect); err != nil {
		return nil, err
	}

	result := streamSelect{}

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

	item, l, err := p.peekWithLengthOrError(ReservedLeftJoin, ReservedWhere, ReservedPartitionBy, ReservedEmit, ReservedEndOfStatement)
	if err != nil {
		return nil, err
	}
	if item == ReservedLeftJoin {
		p.popLength(l)
		j, err := p.parseJoin()
		if err != nil {
			return nil, err
		}
		result.Joins = j
		item, l, err = p.peekWithLengthOrError(ReservedWhere, ReservedPartitionBy, ReservedEmit, ReservedEndOfStatement)
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
		item, l, err = p.peekWithLengthOrError(ReservedPartitionBy, ReservedEmit, ReservedEndOfStatement)
		if err != nil {
			return nil, err
		}
	}
	if item == ReservedPartitionBy {
		p.popLength(l)
		item, err = p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		result.Partition = item
	}

	return &result, err
}
