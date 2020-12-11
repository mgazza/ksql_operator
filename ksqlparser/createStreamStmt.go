package ksqlparser

import (
	"fmt"
	"strings"
)

type createStreamStmt struct {
	stmt
	Columns     *columnDefinitions
	With        *with
	Select      *streamSelect
	EmitChanges bool
}

func (s *createStreamStmt) GetObjectType() CreateObjectType {
	return CreateObjectTypeStream
}

func (s *createStreamStmt) GetActionType() StmtActionType {
	return s.Type
}

func (s *createStreamStmt) String() string {
	sb := []string{string(s.stmt.Type), ReservedStream, s.Name}

	if s.Columns != nil {
		sb = append(sb, s.Columns.String())
	}
	if s.With != nil {
		sb = append(sb, fmt.Sprintf("%s%s %s%s%s", StringOptions.withPrefix, ReservedWith, ReservedOpenParens, s.With.String(), ReservedCloseParens))
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

func (s *createStreamStmt) GetName() string {
	return s.Name
}

func (s *createStreamStmt) GetDataSources() []string {
	var result []string
	if s.Select != nil {
		result = append(result, s.Select.Identifier.Name)
		if s.Select.Joins != nil {
			for _, j := range *s.Select.Joins {
				result = append(result, j.Identifier.Name)
			}
		}
	}
	return result
}
