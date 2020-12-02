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

func (s *createStreamStmt) String() string {
	sb := []string{s.stmt.Type, ReservedStream, s.Name}

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
