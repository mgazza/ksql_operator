package ksqlparser

import (
	"fmt"
	"strings"
)

type columnDefinitions []columnDefinition

func (c columnDefinitions) String() string {
	var f = StringOptions.columnsSeparator

	cols := []string{ReservedOpenParens}
	for i, c := range c {
		if i > 0 {
			cols = append(cols, fmt.Sprintf("%s%s%s", ReservedComma, f, c.String()))
			continue
		}
		cols = append(cols, f, c.String())
	}

	cols = append(cols, f, ReservedCloseParens)
	return strings.Join(cols, "")
}
