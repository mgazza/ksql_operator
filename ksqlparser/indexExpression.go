package ksqlparser

import "fmt"

type indexExpression struct {
	Expression Expression
	Index      Expression
}

func (b *indexExpression) String() string {
	return fmt.Sprintf("%s[%s]", b.Expression.String(), b.Index)
}
