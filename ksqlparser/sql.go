package ksqlparser

import (
	"fmt"
	"strconv"
	"strings"
)

type Stmt interface {
	fmt.Stringer
	GetName() string
	GetDataSources() []string
}

var StringOptions = struct {
	columnsSeparator  string
	withPrefix        string
	selectPrefix      string
	emitPrefix        string
	selectItemPrefix  string
	joinPrefix        string
	fromPrefix        string
	wherePrefix       string
	havingPrefix      string
	groupByPrefix     string
	partitionByPrefix string
	windowPrefix      string
}{
	columnsSeparator:  "\n",
	withPrefix:        "\n",
	selectPrefix:      "\n",
	emitPrefix:        "\n",
	selectItemPrefix:  "\n",
	joinPrefix:        "\n",
	fromPrefix:        "\n",
	wherePrefix:       "\n",
	havingPrefix:      "\n",
	groupByPrefix:     "\n",
	partitionByPrefix: "\n",
	windowPrefix:      "\n",
}

// Parse takes a string representing a SQL stmt and parses it into a stmt.stmt struct. It may fail.
func Parse(sqls string) (Stmt, error) {
	qs, err := ParseMany([]string{sqls})
	if len(qs) == 0 {
		return nil, err
	}
	return qs[0], err
}

// ParseMany takes a string slice representing many SQL queries and parses them into a stmt.stmt struct slice.
// It may fail. If it fails, it will stop at the first failure.
func ParseMany(sqls []string) ([]Stmt, error) {
	var qs []Stmt
	for _, sql := range sqls {
		q, err := parse(sql)
		if err != nil {
			return qs, err
		}
		qs = append(qs, q)
	}
	return qs, nil
}

func parse(sql string) (Stmt, error) {
	return (&parser{sql: strings.TrimSpace(sql)}).parse()
}

type parser struct {
	i   int
	sql string
	//for error debugging
	line int
	col  int
}

func (p *parser) parse() (Stmt, error) {
	q, err := p.doParse()
	if err == nil {
		err = p.validate()
	}
	return q, err
}

func (p *parser) doParse() (Stmt, error) {
	item := p.pop(ReservedCreateOrReplace, ReservedCreate, ReservedReplace, ReservedInsert)

	switch strings.ToUpper(item) {
	case ReservedCreate:
		fallthrough
	case ReservedCreateOrReplace:
		fallthrough
	case ReservedReplace:
		switch strings.ToUpper(p.pop(ReservedTable, ReservedStream)) {
		case ReservedTable:
			n := p.pop()
			if len(n) == 0 {
				return nil, p.Error("[name]")
			}
			stmt := &createTableStmt{
				stmt: stmt{
					Type: item,
					Name: n,
				},
			}

			s, err := p.popOrError(ReservedWith, ReservedAs, ReservedOpenParens)
			if err != nil {
				return nil, err
			}

			if s == ReservedOpenParens {
				// parse column defs
				cols, err := p.parseColumnDefs()
				if err != nil {
					return nil, err
				}
				stmt.Columns = (*columnDefinitions)(cols)
				if s, err = p.popOrError(ReservedWith); err != nil {
					return nil, err
				}
			}
			if s == ReservedWith {
				if _, err := p.popOrError(ReservedOpenParens); err != nil {
					return nil, err
				}

				with, err := p.parseWith(withProperties...)
				if err != nil {
					return nil, err
				}
				stmt.With = with

				s = p.pop(ReservedAs, ReservedEndOfStatement)
			}

			if s == ReservedEndOfStatement {
				return stmt, nil
			}

			if s != ReservedAs && stmt.Columns == nil {
				// theres no as and there was no columnDefinitions def
				return nil, p.Error(ReservedAs)
			}

			// we now have an as
			sel, err := p.parseTableSelect()
			if err != nil {
				return nil, err
			}
			stmt.Select = sel

			s, err = p.popOrError(ReservedEmit, ReservedEndOfStatement)
			if err != nil {
				return nil, err
			}
			if s == ReservedEmit {
				stmt.EmitChanges = true
				s, err = p.popOrError(ReservedEndOfStatement)
			}
			return stmt, nil
		case ReservedStream:
			n := p.pop()
			if len(n) == 0 {
				return nil, p.Error("[name]")
			}
			stmt := &createStreamStmt{
				stmt: stmt{
					Type: item,
					Name: n,
				},
			}
			s, err := p.popOrError(ReservedWith, ReservedAs, ReservedOpenParens)
			if err != nil {
				return nil, err
			}

			if s == ReservedOpenParens {
				// parse column defs
				cols, err := p.parseColumnDefs()
				if err != nil {
					return nil, err
				}
				stmt.Columns = (*columnDefinitions)(cols)
				if s, err = p.popOrError(ReservedWith); err != nil {
					return nil, err
				}
			}
			if s == ReservedWith {
				if _, err := p.popOrError(ReservedOpenParens); err != nil {
					return nil, err
				}

				with, err := p.parseWith(withProperties...)
				if err != nil {
					return nil, err
				}
				stmt.With = with

				s = p.pop(ReservedAs, ReservedEndOfStatement)
			}

			if s == ReservedEndOfStatement {
				return stmt, nil
			}

			if s != ReservedAs && stmt.Columns == nil {
				// theres no as and there was no columnDefinitions def
				return nil, p.Error(ReservedAs)
			}

			// we now have an as
			sel, err := p.parseStreamSelect()
			if err != nil {
				return nil, err
			}
			stmt.Select = sel
			s, err = p.popOrError(ReservedEmit, ReservedEndOfStatement)
			if err != nil {
				return nil, err
			}
			if s == ReservedEmit {
				stmt.EmitChanges = true
				s, err = p.popOrError(ReservedEndOfStatement)
			}
			return stmt, nil

		default:
			return nil, p.Error(fmt.Sprintf("%s or %s", ReservedTable, ReservedStream))
		}

	case ReservedInsert:
		// TODO
		n := p.pop()
		if len(n) == 0 {
			return nil, p.Error("[name]")
		}
		stmt := &insertIntoStmt{
			stmt: stmt{
				Type: item,
				Name: n,
			},
		}

		sel, err := p.parseStreamSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = sel
		_, err = p.popOrError(ReservedEndOfStatement)
		return stmt, nil
	default:
		return nil, p.Error(fmt.Sprintf("%s or %s or %s or %s", ReservedCreate, ReservedCreateOrReplace, ReservedReplace, ReservedInsert))
	}
}

func (p *parser) validate() error {
	return nil
}

func (p *parser) parseNumber() (int, error) {
	a := p.pop()
	i, err := strconv.Atoi(a)
	if err != nil {
		return i, p.Error("NUMBER")
	}
	return i, nil
}
