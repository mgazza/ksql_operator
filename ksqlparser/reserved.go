package ksqlparser

const (
	// ReservedSelect represents a SELECT stmt
	ReservedSelect = "SELECT"
	// ReservedInsert represents an INSERT stmt
	ReservedInsert = "INSERT INTO"
	// ReservedCreate represents an CREATE stmt
	ReservedCreate = "CREATE"
	// ReservedCreateOrReplace represents an CREATE OR REPLACE stmt
	ReservedCreateOrReplace = "CREATE OR REPLACE"
	// ReservedReplace represents an REPLACE stmt
	ReservedReplace = "REPLACE"

	// ReservedEq -> "="
	ReservedEq = "="
	// ReservedNe -> "!="
	ReservedNe = "!="
	// ReservedGt -> ">"
	ReservedGt = ">"
	// ReservedLt -> "<"
	ReservedLt = "<"
	// ReservedGte -> ">="
	ReservedGte = ">="
	// ReservedLte -> "<="
	ReservedLte = "<="

	// ReservedIsNot -> "IS NOT"
	ReservedIsNot = "IS NOT"
	// ReservedLike -> "LIKE"
	ReservedLike = "LIKE"

	// ReservedOpenParens -> "("
	ReservedOpenParens = "("
	// ReservedCloseParens -> ")"
	ReservedCloseParens = ")"

	// ReservedOpenCrotchet -> "["
	ReservedOpenCrotchet = "["
	// ReservedCloseCrotchet -> "]"
	ReservedCloseCrotchet = "]"

	// ReservedPlus -> "+"
	ReservedPlus = "+"
	// ReservedMinus -> "-"
	ReservedMinus = "-"
	// ReservedMultiply -> "*"
	ReservedMultiply = "*"
	// ReservedDivide -> "/"
	ReservedDivide = "/"

	// ReservedTable represents a TABLE keyword
	ReservedTable = "TABLE"
	// ReservedStream represents a STREAM keyword
	ReservedStream = "STREAM"
	// ReservedWith represents a WITH keyword
	ReservedWith = "WITH"
	// ReservedWhere represents a WHERE keyword
	ReservedWhere = "WHERE"
	// ReservedFrom represents a FROM keyword
	ReservedFrom = "FROM"
	// ReservedAs represents a AS keyword
	ReservedAs = "AS"
	// ReservedEndOfStatement represents a ; keyword
	ReservedEndOfStatement = ";"
	// ReservedPartitionBy represents a PARTITION BY keyword
	ReservedPartitionBy = "PARTITION BY"
	// ReservedGroupBy represents a GROUP BY keyword
	ReservedGroupBy = "GROUP BY"
	// ReservedHaving represents a HAVING keyword
	ReservedHaving = "HAVING"
	// ReservedEmit represents a EMIT CHANGES keyword
	ReservedEmit = "EMIT CHANGES"

	// ReservedWindow represents a WINDOW keyword
	ReservedWindow = "WINDOW"
	// ReservedLeftJoin represents a LEFT JOIN keyword
	ReservedLeftJoin = "LEFT JOIN"
	// ReservedOn represents a ON keyword
	ReservedOn = "ON"

	// ReservedComma represents a ,
	ReservedComma = ","

	// ReservedAnd represents a AND keyword
	ReservedAnd = "AND"
	// ReservedOr represents a OR keyword
	ReservedOr = "OR"

	// ReservedCaseWhen represents a CASE WHEN keyword
	ReservedCaseWhen = "CASE WHEN"
	// ReservedThen represents a THEN keyword
	ReservedThen = "THEN"
	// ReservedElse represents a ELSE keyword
	ReservedElse = "ELSE"
	// ReservedEnd represents a END keyword
	ReservedEnd = "END"

	// ReservedPrimaryKey represents a PRIMARY KEY keyword
	ReservedPrimaryKey = "PRIMARY KEY"
	// ReservedKey represents a KEY keyword
	ReservedKey = "KEY"
)

var reservedWords = []string{
	ReservedOpenParens,
	ReservedCloseParens,
	ReservedGte,
	ReservedLte,
	ReservedNe,
	ReservedComma,
	ReservedEq,
	ReservedGt,
	ReservedLt,
	ReservedSelect,
	ReservedInsert,
	ReservedCreate,
	ReservedCreateOrReplace,
	ReservedReplace,
	ReservedStream,
	ReservedTable,
	ReservedWith,
	ReservedWhere,
	ReservedFrom,
	ReservedAs,
	ReservedPartitionBy,
	ReservedGroupBy,
	ReservedHaving,
	ReservedWindow,
	ReservedLeftJoin,
}
