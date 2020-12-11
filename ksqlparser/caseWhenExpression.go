package ksqlparser

import "strings"

type caseWhenExpression struct {
	When []*Condition
	Then Expression
	Else Expression
}

func (b *caseWhenExpression) String() string {
	sb := []string{ReservedCaseWhen}
	for _, w := range b.When {
		sb = append(sb, w.String())
	}
	sb = append(sb, ReservedThen, b.Then.String())
	if b.Else != nil {
		sb = append(sb, ReservedElse, b.Else.String())
	}
	sb = append(sb, ReservedEnd)
	return strings.Join(sb, " ")
}
