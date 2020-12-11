package ksqlparser

const (
	// scalar functions
	FunctionTypeAbs               = "ABS"
	FunctionTypeArrayContains     = "ARRAYCONTAINS"
	FunctionTypeCeil              = "CEIL"
	FunctionTypeConcat            = "CONCAT"
	FunctionTypeExtractJsonField  = "EXTRACTJSONFIELD"
	FunctionTypeFloor             = "FLOOR"
	FunctionTypeIfNull            = "IFNULL"
	FunctionTypeLCase             = "LCASE"
	FunctionTypeLen               = "LEN"
	FunctionTypeRandom            = "RANDOM"
	FunctionTypeRound             = "ROUND"
	FunctionTypeStringToTimeStamp = "STRINGTOTIMESTAMP"
	FunctionTypeSubString         = "SUBSTRING"
	FunctionTypeTimeStampToString = "TIMESTAMPTOSTRING"
	FunctionTypeTrim              = "TRIM"
	FunctionTypeUCase             = "UCASE"

	// aggregate functions
	FunctionTypeCount         = "COUNT"
	FunctionTypeCountDistinct = "COUNT_DISTINCT"
	FunctionTypeMax           = "MAX"
	FunctionTypeMin           = "MIN"
	FunctionTypeSum           = "SUM"
	FunctionTypeTopK          = "TOPK"
	FunctionTypeTopKDistinct  = "TOPKDISTINCT"

	//other undocumented functions
	FunctionExplode          = "EXPLODE"
	FunctionAsMap            = "AS_MAP"
	FunctionAsValue          = "AS_VALUE"
	FunctionLatestByOffset   = "LATEST_BY_OFFSET"
	FunctionEarliestByOffset = "EARLIEST_BY_OFFSET"
	FunctionCollectList      = "COLLECT_LIST"

	FunctionCast  = "CAST"
	FunctionSplit = "SPLIT"
)

var functionParamMap = map[string]int{
	FunctionTypeAbs:               1,
	FunctionTypeArrayContains:     2,
	FunctionTypeCeil:              1,
	FunctionTypeConcat:            2,
	FunctionTypeExtractJsonField:  2,
	FunctionTypeFloor:             1,
	FunctionTypeIfNull:            2,
	FunctionTypeLCase:             1,
	FunctionTypeLen:               1,
	FunctionTypeRandom:            0,
	FunctionTypeRound:             1,
	FunctionTypeStringToTimeStamp: 2,
	FunctionTypeSubString:         3,
	FunctionTypeTimeStampToString: 2,
	FunctionTypeTrim:              1,
	FunctionTypeUCase:             1,

	FunctionTypeCount:         1,
	FunctionTypeCountDistinct: 1,
	FunctionTypeMax:           1,
	FunctionTypeMin:           1,
	FunctionTypeSum:           1,
	FunctionTypeTopK:          2,
	FunctionTypeTopKDistinct:  2,

	FunctionExplode:          1,
	FunctionAsMap:            2,
	FunctionAsValue:          1,
	FunctionLatestByOffset:   1,
	FunctionEarliestByOffset: 1,
	FunctionCollectList:      1,
	FunctionCast:             1,
	FunctionSplit:            2,
}

// N.B. order is important
var functionTypes = []string{
	FunctionTypeAbs,
	FunctionTypeArrayContains,
	FunctionTypeCeil,
	FunctionTypeConcat,
	FunctionTypeExtractJsonField,
	FunctionTypeFloor,
	FunctionTypeIfNull,
	FunctionTypeLCase,
	FunctionTypeLen,
	FunctionTypeRandom,
	FunctionTypeRound,
	FunctionTypeStringToTimeStamp,
	FunctionTypeSubString,
	FunctionTypeTimeStampToString,
	FunctionTypeTrim,
	FunctionTypeUCase,

	FunctionTypeCountDistinct,
	FunctionTypeCount,
	FunctionTypeMax,
	FunctionTypeMin,
	FunctionTypeSum,
	FunctionTypeTopK,
	FunctionTypeTopKDistinct,

	FunctionExplode,
	FunctionAsMap,
	FunctionAsValue,
	FunctionLatestByOffset,
	FunctionEarliestByOffset,
	FunctionCollectList,
	FunctionCast,
	FunctionSplit,

	ReservedCaseWhen,
}
