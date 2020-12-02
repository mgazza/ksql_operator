package ksqlparser

func (p *parser) parseConditions() ([]*Condition, error) {
	var result []*Condition
	for {
		condition := Condition{}
		le, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		o, err := p.popOrError(operators...)
		if err != nil {
			return nil, err
		}
		re, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		condition.Operand1 = le
		condition.Operator = Operator(o)
		condition.Operand2 = re
		result = append(result, &condition)
		i, l := p.peekWithLength(ReservedAnd, ReservedOr)
		switch i {
		case ReservedAnd:
			p.popLength(l)
			condition.Conjunction = i
		case ReservedOr:
			p.popLength(l)
			condition.Conjunction = i
		default:
			return result, nil
		}
	}
}
