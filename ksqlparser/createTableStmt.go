package ksqlparser

import (
	"fmt"
	"strings"
)

type createTableStmt struct {
	stmt
	Columns     *columnDefinitions
	With        *with
	Select      *tableSelect
	EmitChanges bool
}

func (s *createTableStmt) String() string {
	sb := []string{s.stmt.Type, ReservedTable, s.Name}

	if s.Columns != nil {
		sb = append(sb, s.Columns.String())
	}
	if s.With != nil {
		sb = append(sb, ReservedWith, ReservedOpenParens, s.With.String(), ReservedCloseParens)
	}
	if s.Select != nil {
		sb = append(sb, ReservedAs, fmt.Sprintf("%s%s", StringOptions.selectPrefix, ReservedSelect), s.Select.String())
	}
	if s.EmitChanges {
		sb = append(sb, fmt.Sprintf("%s%s", StringOptions.emitPrefix, ReservedEmit))
	}
	sb = append(sb, ReservedEndOfStatement)
	return strings.Join(sb, " ")
}

func (s *createTableStmt) GetName() string {
	return s.Name
}

func (s *createTableStmt) GetDataSources() []string {
	var result []string
	if s.Select != nil {
		result = append(result, s.Select.Identifier.Name)
	}
	return result
}
