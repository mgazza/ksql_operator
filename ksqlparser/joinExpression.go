package ksqlparser

import "strings"

type joinExpression struct {
	Identifier identifier
	Conditions []*Condition
}

func (e *joinExpression) String() string {
	sb := []string{ReservedLeftJoin, e.Identifier.String(), ReservedOn}
	for _, c := range e.Conditions {
		sb = append(sb, c.String())
	}
	return strings.Join(sb, " ")
}

func (p *parser) parseJoin() (*[]joinExpression, error) {
	var result []joinExpression
	for {
		expr := joinExpression{}
		i, err := p.parseIdentifierWithAlias()
		if err != nil {
			return nil, err
		}
		expr.Identifier = *i

		_, err = p.popOrError(ReservedOn)
		if err != nil {
			return nil, err
		}
		expr.Conditions, err = p.parseConditions()
		if err != nil {
			return nil, err
		}
		result = append(result, expr)
		next, l := p.peekWithLength(ReservedLeftJoin)
		if next == ReservedLeftJoin {
			p.popLength(l)
			continue
		}
		return &result, nil
	}
}
