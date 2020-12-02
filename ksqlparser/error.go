package ksqlparser

import "fmt"

func (p *parser) Error(expected string) error {
	return fmt.Errorf("expected %s at line %d col %d, %s^", expected, p.line, p.col, p.sql[:p.i])
}

func (p *parser) SyntaxError() error {
	return fmt.Errorf("syntax error at line %d col %d, %s^", p.line, p.col, p.sql[:p.i])
}
