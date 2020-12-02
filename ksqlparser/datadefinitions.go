package ksqlparser

import (
	"fmt"
	"strings"
)

type DataType string

const (
	DataTypeBool    = "BOOLEAN"
	DataTypeDouble  = "DOUBLE"
	DataTypeInt     = "INTEGER"
	DataTypeBigInt  = "BIGINT"
	DataTypeVarchar = "VARCHAR"
	DataTypeString  = "STRING"
	DataTypeArray   = "ARRAY"
	DataTypeMap     = "MAP"
	DataTypeStruct  = "STRUCT"
)

var basicDataTypes = []string{
	DataTypeBool,
	DataTypeInt,
	DataTypeBigInt,
	DataTypeDouble,
	DataTypeBool,
	DataTypeVarchar,
	DataTypeString,
}

var dataTypes = []string{
	DataTypeBool,
	DataTypeInt,
	DataTypeBigInt,
	DataTypeBool,
	DataTypeDouble,
	DataTypeVarchar,
	DataTypeString,
	DataTypeArray,
	DataTypeMap,
	DataTypeStruct,
}

type dataTypeDefinition interface {
	fmt.Stringer
}

type simpleDataType struct {
	Type string
}

type arrayTypeDataType struct {
	ItemType dataTypeDefinition
}

type mapTypeDataType struct {
	KeyType   dataTypeDefinition
	ValueType dataTypeDefinition
}

type structField struct {
	Name string
	Type dataTypeDefinition
}

type structTypeDataType struct {
	Fields []structField
}

func (s *simpleDataType) String() string {
	return s.Type
}

func (s *arrayTypeDataType) String() string {
	return fmt.Sprintf("%s<%s>", DataTypeArray, s.ItemType.String())
}

func (s *mapTypeDataType) String() string {
	return fmt.Sprintf("%s<%s, %s>", DataTypeMap, s.KeyType.String(), s.ValueType.String())
}

func (s *structTypeDataType) String() string {
	var sb []string
	for _, f := range s.Fields {
		sb = append(sb, f.String())
	}
	return fmt.Sprintf("%s<%s>", DataTypeStruct, strings.Join(sb, ", "))
}

func (s *structField) String() string {
	return fmt.Sprintf("%s %s", s.Name, s.Type.String())
}

func (p *parser) parseDataType() (dataTypeDefinition, error) {
	dataType, err := p.popOrError(dataTypes...)
	if err != nil {
		return nil, err
	}
	switch dataType {
	case DataTypeArray:
		if s := p.pop(ReservedLt); s != ReservedLt {
			return nil, p.Error(ReservedLt)
		}
		itemType, err := p.parseDataType()
		if err != nil {
			return nil, err
		}
		result := &arrayTypeDataType{
			ItemType: itemType,
		}
		if e := p.pop(ReservedGt); e != ReservedGt {
			return nil, p.Error(ReservedGt)
		}
		return result, nil
	case DataTypeMap:
		if s := p.pop(ReservedLt); s != ReservedLt {
			return nil, p.Error(ReservedLt)
		}
		keyType, err := p.parseDataType()
		if err != nil {
			return nil, err
		}

		if c := p.pop(ReservedComma); c != ReservedComma {
			return nil, p.Error(ReservedComma)
		}
		valueType, err := p.parseDataType()
		if err != nil {
			return nil, err
		}
		if e := p.pop(ReservedGt); e != ReservedGt {
			return nil, p.Error(ReservedGt)
		}
		return &mapTypeDataType{
			KeyType:   keyType,
			ValueType: valueType,
		}, nil
	case DataTypeStruct:
		result := &structTypeDataType{
			Fields: []structField{},
		}
		if s := p.pop(ReservedLt); s != ReservedLt {
			return nil, p.Error(ReservedLt)
		}
		for {
			n, l := p.peekWithLength()
			if l == 0 || !isIdentifier(n) {
				return nil, p.Error("[name]")
			}
			p.popLength(l)
			t, err := p.parseDataType()
			if err != nil {
				return nil, err
			}
			result.Fields = append(result.Fields, structField{
				Name: n,
				Type: t,
			})
			i, err := p.popOrError(ReservedComma, ReservedGt)
			if i == ReservedComma {
				continue
			}
			return result, nil
		}
	case DataTypeInt:
		fallthrough
	case DataTypeBool:
		fallthrough
	case DataTypeVarchar:
		fallthrough
	case DataTypeDouble:
		fallthrough
	case DataTypeBigInt:
		fallthrough
	case DataTypeString:
		return &simpleDataType{
			Type: dataType,
		}, nil
	}
	return nil, fmt.Errorf("unhandled DataType %s", dataType)
}
