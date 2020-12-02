package ksqlparser

type basicExpression struct {
	Name string
}

func (b *basicExpression) String() string {
	return b.Name
}
