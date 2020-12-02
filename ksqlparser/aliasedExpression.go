package ksqlparser

import "fmt"

type aliasedExpression struct {
	Expression Expression
	Alias      string
}

func (e aliasedExpression) String() string {
	if e.Alias != "" {
		return fmt.Sprintf("%s %s %s", e.Expression.String(), ReservedAs, e.Alias)
	}
	return e.Expression.String()
}
