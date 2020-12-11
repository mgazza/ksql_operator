package ksqlparser

type stmt struct {
	Type StmtActionType
	Name string
}

type StmtActionType string

const (
	// StmtTypeSelect represents a SELECT stmt
	StmtTypeSelect = StmtActionType(ReservedSelect)
	// StmtTypeInsert represents an INSERT stmt
	StmtTypeInsert = StmtActionType(ReservedInsert)
	// StmtTypeCreate represents an CREATE stmt
	StmtTypeCreate = StmtActionType(ReservedCreate)
	// StmtTypeCreateOrReplace represents an CREATE OR REPLACE stmt
	StmtTypeCreateOrReplace = StmtActionType(ReservedCreateOrReplace)
	// StmtTypeReplace represents an REPLACE stmt
	StmtTypeReplace = StmtActionType(ReservedReplace)
)

type CreateObjectType string

const (
	CreateObjectTypeTable  = CreateObjectType(ReservedTable)
	CreateObjectTypeStream = CreateObjectType(ReservedStream)
)
