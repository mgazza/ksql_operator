package ksqlparser

import "fmt"

type insertIntoStmt struct {
	stmt
	Select *streamSelect
}

func (s *insertIntoStmt) String() string {
	return fmt.Sprintf("%s %s %s %s %s", s.stmt.Type, s.Name, ReservedSelect, s.Select.String(), ReservedEndOfStatement)
}
