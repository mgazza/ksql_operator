package ksqlparser

import (
	"fmt"
	"strings"
)

type aliasedExpressions []*aliasedExpression

func (ae *aliasedExpressions) String() string {
	var f = StringOptions.selectItemPrefix

	var expressions []string
	for i, e := range *ae {
		if i > 0 {
			expressions = append(expressions, fmt.Sprintf("%s%s%s", f, ReservedComma, e.String()))
			continue
		}
		expressions = append(expressions, f, e.String())
	}
	return strings.Join(expressions, "")
}
