package ksqlparser

import "fmt"

type operatorExpression struct {
	LeftExpression  Expression
	RightExpression Expression
	Operator        string
}

func (b *operatorExpression) String() string {
	return fmt.Sprintf("%s %s %s", b.LeftExpression.String(), b.Operator, b.RightExpression.String())
}
