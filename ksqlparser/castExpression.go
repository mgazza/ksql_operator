package ksqlparser

import "fmt"

type castExpression struct {
	InnerExpression Expression
	DataType        dataTypeDefinition
}

func (b *castExpression) String() string {
	return fmt.Sprintf("%s(%s %s %s)", FunctionCast, b.InnerExpression.String(), ReservedAs, b.DataType.String())
}
