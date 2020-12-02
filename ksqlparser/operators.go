package ksqlparser

import "strings"

type Operator string

var operators = []string{
	ReservedGte,
	ReservedLte,
	ReservedNe,
	ReservedComma,
	ReservedEq,
	ReservedGt,
	ReservedLt,
	ReservedIsNot,
	ReservedLike,
}

// Condition is a single boolean condition in a WHERE clause
type Condition struct {
	// Operand1 is the left hand side operand
	Operand1 Expression
	// Operator is e.g. "=", ">"
	Operator Operator
	// Operand1 is the right hand side operand
	Operand2 Expression
	// Conjunction is a following AND or OR
	Conjunction string
}

func (c Condition) String() string {
	var sb []string
	sb = append(sb, c.Operand1.String(), string(c.Operator), c.Operand2.String())
	return strings.Join(sb, " ")
}
