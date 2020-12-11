package ksqlparser

import (
	"fmt"
	"strings"
)

type functionExpression struct {
	Name   string
	Params []Expression
}

func (b *functionExpression) String() string {
	var sb []string
	for _, s := range b.Params {
		sb = append(sb, s.String())
	}
	return fmt.Sprintf("%s%s%s%s", b.Name, ReservedOpenParens, strings.Join(sb, ", "), ReservedCloseParens)
}
