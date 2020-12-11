package ksqlparser

import (
	"fmt"
	"strings"
)

type columnDefinition struct {
	Name      string
	DataType  dataTypeDefinition
	IsPrimary bool
	IsKey     bool
}

func (d *columnDefinition) String() string {
	sb := []string{d.Name, d.DataType.String()}

	// TODO change these into a single field
	if d.IsKey {
		sb = append(sb, ReservedKey)
	}
	if d.IsPrimary {
		sb = append(sb, ReservedPrimaryKey)
	}

	return strings.Join(sb, " ")
}

func (p *parser) parseColumnDefs() (*[]columnDefinition, error) {
	var result []columnDefinition
	for {
		item := p.pop()
		if !isIdentifier(item) {
			return nil, p.Error("[field]")
		}
		def := columnDefinition{
			Name: item,
		}
		dt, err := p.parseDataType()
		if err != nil {
			return nil, err
		}
		def.DataType = dt

		// check for key meta
		item, l := p.peekWithLength(ReservedPrimaryKey, ReservedKey)
		switch strings.ToUpper(item) {
		case ReservedPrimaryKey:
			p.popLength(l)
			def.IsPrimary = true
		case ReservedKey:
			p.popLength(l)
			def.IsKey = true
		}

		result = append(result, def)

		// check for a , or a )
		switch p.pop(ReservedCloseParens, ReservedComma) {
		case ReservedCloseParens:
			return &result, nil
		case ReservedComma:
			continue
		default:
			return nil, p.Error(fmt.Sprintf("%s or %s", ReservedCloseParens, ReservedComma))
		}
	}
}
