package ksqlparser

import "fmt"

type identifier struct {
	Name  string
	Alias string
}

func (i identifier) String() string {
	if i.Alias != "" {
		return fmt.Sprintf("%s %s %s", i.Name, ReservedAs, i.Alias)
	}
	return i.Name
}

func (p *parser) parseIdentifier() (string, error) {
	item := p.pop()
	if len(item) == 0 || !isIdentifier(item) {
		return "", p.Error("[identifier]")
	}

	return item, nil
}

func (p *parser) parseIdentifierWithAlias() (*identifier, error) {
	i, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	result := &identifier{
		Name: i,
	}
	item, l := p.peekWithLength(ReservedAs)
	if item == ReservedAs {
		p.popLength(l)
		item := p.pop()
		if len(item) == 0 || !isIdentifier(item) {
			return nil, p.Error("[alias]")
		}
		result.Alias = item
	}
	return result, nil
}
