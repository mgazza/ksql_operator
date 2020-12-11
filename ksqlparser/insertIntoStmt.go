package ksqlparser

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

type insertIntoStmt struct {
	stmt
	Select *streamSelect
}

func (s *insertIntoStmt) GetActionType() StmtActionType {
	return s.Type
}

func (s *insertIntoStmt) GetName() string {
	hasher := sha256.New()
	hasher.Write([]byte(s.String()))
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

func (s *insertIntoStmt) GetDataSources() []string {
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

func (s *insertIntoStmt) String() string {
	return fmt.Sprintf("%s %s %s %s %s", s.stmt.Type, s.Name, ReservedSelect, s.Select.String(), ReservedEndOfStatement)
}
