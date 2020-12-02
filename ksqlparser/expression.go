package ksqlparser

import (
	"fmt"
	"strings"
)

type Expression interface {
	fmt.Stringer
}

func (p *parser) parseExpressionList(endKeywords ...string) (*[]*aliasedExpression, error) {
	var result []*aliasedExpression

	for {
		e, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		expr := &aliasedExpression{
			Expression: e,
			Alias:      "",
		}

		item, l := p.peekWithLength(append(endKeywords, ReservedAs, ReservedComma)...)

		result = append(result, expr)

		for {
			if item == ReservedComma {
				//break the loop
				break
			}

			if arrayContains(endKeywords, strings.ToUpper(item)) {
				return &result, nil // done parsing expressions don't pop so the keyword remains
			}

			if item == ReservedAs {
				p.popLength(l)
			}

			expr.Alias = p.pop()
			if !isIdentifier(expr.Alias) {
				return nil, p.Error("alias")
			}

			item, l, err = p.peekWithLengthOrError(append(endKeywords, ReservedComma)...)
			if err != nil {
				return nil, err
			}

		}

		if item == ReservedComma {
			p.popLength(l)
			continue
		}
	}
}

func (p *parser) parseExpression() (Expression, error) {
	item, l := p.peekWithLength(functionTypes...)
	if l == 0 {
		return nil, p.Error("expression")
	}
	p.popLength(l)

	if strings.ToUpper(item) == ReservedCaseWhen {
		cwe := &caseWhenExpression{}

		when, err := p.parseConditions()
		if err != nil {
			return nil, err
		}
		cwe.When = when
		_, err = p.popOrError(ReservedThen)
		if err != nil {
			return nil, err
		}
		then, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		cwe.Then = then
		i, err := p.popOrError(ReservedElse, ReservedEnd)
		if strings.ToUpper(i) == ReservedElse {
			els, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			cwe.Else = els
			i, err = p.popOrError(ReservedEnd)
		}
		return cwe, nil
	}

	var result Expression = &basicExpression{
		Name: item,
	}

	next, l := p.peekWithLength(ReservedComma,
		ReservedOpenParens,
		ReservedCloseParens,
		ReservedPlus,
		ReservedMinus,
		ReservedMultiply,
		ReservedDivide,
		ReservedAs,
		ReservedOpenCrotchet,
		ReservedCloseCrotchet)

	if next == ReservedOpenParens {
		p.popLength(l) // consume the (
		fn, err := p.parseFunction(item)
		if err != nil {
			return nil, err
		}

		result = fn

		next, l = p.peekWithLength(ReservedComma,
			ReservedCloseParens,
			ReservedPlus,
			ReservedMinus,
			ReservedMultiply,
			ReservedDivide,
			ReservedOpenCrotchet,
			ReservedCloseCrotchet)

	}

	if next == ReservedOpenCrotchet {
		p.popLength(l)
		index, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		_, err = p.popOrError(ReservedCloseCrotchet)
		if err != nil {
			return nil, err
		}
		result = &indexExpression{
			Expression: result,
			Index:      index,
		}
		next, l = p.peekWithLength(ReservedComma,
			ReservedCloseParens,
			ReservedPlus,
			ReservedMinus,
			ReservedMultiply,
			ReservedDivide)
	}

	switch strings.ToUpper(next) {
	case ReservedPlus:
		fallthrough
	case ReservedMinus:
		fallthrough
	case ReservedMultiply:
		fallthrough
	case ReservedDivide:
		p.popLength(l)
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		return &operatorExpression{
			LeftExpression:  result,
			Operator:        next,
			RightExpression: expr,
		}, nil
	}

	return result, nil
}

func (p *parser) parseFunction(fname string) (Expression, error) {
	if strings.ToUpper(fname) == FunctionCast {
		// read the expression
		iexpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		_, err = p.popOrError(ReservedAs)
		if err != nil {
			return nil, err
		}
		dt, err := p.parseDataType()
		if err != nil {
			return nil, err
		}
		_, err = p.popOrError(ReservedCloseParens)
		if err != nil {
			return nil, err
		}
		return &castExpression{
			InnerExpression: iexpr,
			DataType:        dt,
		}, nil
	}

	var params []Expression

	next, l := p.peekWithLength(ReservedCloseParens)
	// check for end of params
	if next == ReservedCloseParens {
		p.popLength(l)
		return &functionExpression{
			Name:   fname,
			Params: params,
		}, nil
	}

	for {

		innerExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		params = append(params, innerExpr)

		next, err := p.popOrError(ReservedComma, ReservedCloseParens)
		if err != nil {
			return nil, err
		}

		if next == ReservedCloseParens {
			return &functionExpression{
				Name:   fname,
				Params: params,
			}, nil
		}
	}
}
