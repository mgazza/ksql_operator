package ksqlparser

import (
	"github.com/go-test/deep"
	"testing"
)

func TestParse(t *testing.T) {
	type args struct {
		sqls string
	}
	tests := []struct {
		name    string
		args    args
		want    Stmt
		wantErr bool
		debug   bool
	}{
		// create table with as select with single column
		{
			name: "create table with as select with single column",
			args: args{
				sqls: "CREATE TABLE table AS SELECT column FROM tbl EMIT CHANGES;",
			},
			want: &createTableStmt{
				stmt: stmt{
					Type: "CREATE",
					Name: "table",
				},
				Select: &tableSelect{
					Expressions: []*aliasedExpression{
						{
							Expression: &basicExpression{
								Name: "column",
							},
							Alias: "",
						},
					},
					Identifier: identifier{
						Name:  "tbl",
						Alias: "",
					},
					Window: nil,
					Where:  nil,
					Group:  nil,
					Having: nil,
				},
				EmitChanges: true,
			},
			wantErr: false,
		},
		// create table with as select with single column with alias
		{
			name: "create table with as select with single column with alias",
			args: args{
				sqls: "CREATE TABLE table AS SELECT column as alias FROM tbl EMIT CHANGES;",
			},
			want: &createTableStmt{
				stmt: stmt{
					Type: "CREATE",
					Name: "table",
				},
				Select: &tableSelect{
					Expressions: []*aliasedExpression{
						{
							Expression: &basicExpression{
								Name: "column",
							},
							Alias: "alias",
						},
					},
					Identifier: identifier{
						Name:  "tbl",
						Alias: "",
					},
					Window: nil,
					Where:  nil,
					Group:  nil,
					Having: nil,
				},
				EmitChanges: true,
			},
			wantErr: false,
		},
		// create table with as select with single column with alias and asterisk
		{
			name: "create table with as select with single column with alias and asterisk",
			args: args{
				sqls: "CREATE TABLE table AS SELECT column AS alias, * FROM tbl EMIT CHANGES;",
			},
			want: &createTableStmt{
				stmt: stmt{
					Type: "CREATE",
					Name: "table",
				},
				Select: &tableSelect{
					Expressions: []*aliasedExpression{
						{
							Expression: &basicExpression{
								Name: "column",
							},
							Alias: "alias",
						},
						{
							Expression: &basicExpression{
								Name: "*",
							},
							Alias: "",
						},
					},
					Identifier: identifier{
						Name:  "tbl",
						Alias: "",
					},
					Window: nil,
					Where:  nil,
					Group:  nil,
					Having: nil,
				},
				EmitChanges: true,
			},
			wantErr: false,
		},
		// create table with column definitions and options
		{
			name: "create table with column definitions and options",
			args: args{
				sqls: "CREATE TABLE table ( column1 string, column2 ARRAY<string>, column3 MAP<string,string> ) " +
					"WITH (kafka_topic='topic', value_format='JSON', PARTITIONS=1, REPLICAS=1);",
			},
			want: &createTableStmt{
				stmt: stmt{
					Type: "CREATE",
					Name: "table",
				},
				Columns: &columnDefinitions{
					{
						Name: "column1",
						DataType: &simpleDataType{
							Type: "STRING",
						},
						IsPrimary: false,
					},
					{
						Name: "column2",
						DataType: &arrayTypeDataType{
							ItemType: &simpleDataType{Type: DataTypeString},
						},
						IsPrimary: false,
					},
					{
						Name: "column3",
						DataType: &mapTypeDataType{
							KeyType:   &simpleDataType{Type: DataTypeString},
							ValueType: &simpleDataType{Type: DataTypeString},
						},
						IsPrimary: false,
					},
				},
				With: &with{
					KafkaTopic:  "'topic'",
					ValueFormat: ValueFormatJson,
					Partitions:  1,
					Replicas:    1,
					TimeStamp:   "",
					Key:         "",
				},
				Select: nil,
			},
			wantErr: false,
		},
		// create stream with column definitions and options
		{
			name: "create stream with column definitions and options",
			args: args{
				sqls: "CREATE STREAM stream ( column1 string, column2 ARRAY<string>, column3 MAP<string,string> ) " +
					"WITH (kafka_topic='topic', value_format='JSON', PARTITIONS=1, REPLICAS=1);",
			},
			want: &createStreamStmt{
				stmt: stmt{
					Type: "CREATE",
					Name: "table",
				},
				Columns: &columnDefinitions{
					{
						Name: "column1",
						DataType: &simpleDataType{
							Type: "STRING",
						},
						IsPrimary: false,
					},
					{
						Name: "column2",
						DataType: &arrayTypeDataType{
							ItemType: &simpleDataType{Type: DataTypeString},
						},
						IsPrimary: false,
					},
					{
						Name: "column3",
						DataType: &mapTypeDataType{
							KeyType:   &simpleDataType{Type: DataTypeString},
							ValueType: &simpleDataType{Type: DataTypeString},
						},
						IsPrimary: false,
					},
				},
				With: &with{
					KafkaTopic:  "'topic'",
					ValueFormat: ValueFormatJson,
					Partitions:  1,
					Replicas:    1,
					TimeStamp:   "",
					Key:         "",
				},
				Select: nil,
			},
			wantErr: false,
			debug:   true,
		},
		// create stream with as select with single column
		{
			name: "create stream with as select with single column",
			args: args{
				sqls: "CREATE STREAM stream AS SELECT column FROM tbl EMIT CHANGES;",
			},
			want: &createStreamStmt{
				stmt: stmt{
					Type: "CREATE",
					Name: "stream",
				},
				Select: &streamSelect{
					Expressions: []*aliasedExpression{
						{
							Expression: &basicExpression{
								Name: "column",
							},
							Alias: "",
						},
					},
					Identifier: identifier{
						Name:  "tbl",
						Alias: "",
					},
					Where: nil,
				},
				EmitChanges: true,
			},
			wantErr: false,
		},
		// create stream with as select with single column and left join
		{
			name: "create stream with as select with single column and left join",
			args: args{
				sqls: "CREATE STREAM stream AS SELECT column FROM tbl LEFT JOIN tbl2 on tbl1.field=tbl2.field EMIT CHANGES;",
			},
			want: &createStreamStmt{
				stmt: stmt{
					Type: "CREATE",
					Name: "stream",
				},
				Select: &streamSelect{
					Expressions: []*aliasedExpression{
						{
							Expression: &basicExpression{
								Name: "column",
							},
							Alias: "",
						},
					},
					Identifier: identifier{
						Name:  "tbl",
						Alias: "",
					},
					Joins: &[]joinExpression{
						{
							Identifier: identifier{
								Name:  "tbl2",
								Alias: "",
							},
							Conditions: []*Condition{
								{
									Operand1: &basicExpression{
										Name: "tbl1.field",
									},
									Operator: ReservedEq,
									Operand2: &basicExpression{
										Name: "tbl2.field",
									},
									Conjunction: "",
								},
							},
						},
					},
					Where: nil,
				},
				EmitChanges: true,
			},
			wantErr: false,
		},
		// create stream with as select with single column and left join and where
		{
			name: "create stream with as select with single column and left join and where",
			args: args{
				sqls: "CREATE STREAM stream AS SELECT column FROM tbl LEFT JOIN tbl2 on tbl.field=tbl2.field " +
					"WHERE tbl.field=1 EMIT CHANGES;",
			},
			want: &createStreamStmt{
				stmt: stmt{
					Type: "CREATE",
					Name: "stream",
				},
				Select: &streamSelect{
					Expressions: []*aliasedExpression{
						{
							Expression: &basicExpression{
								Name: "column",
							},
							Alias: "",
						},
					},
					Identifier: identifier{
						Name:  "tbl",
						Alias: "",
					},
					Joins: &[]joinExpression{
						{
							Identifier: identifier{
								Name:  "tbl2",
								Alias: "",
							},
							Conditions: []*Condition{
								{
									Operand1: &basicExpression{
										Name: "tbl.field",
									},
									Operator: ReservedEq,
									Operand2: &basicExpression{
										Name: "tbl2.field",
									},
									Conjunction: "",
								},
							},
						},
					},
					Where: &[]*Condition{
						{
							Operand1: &basicExpression{
								Name: "tbl.field",
							},
							Operator: "=",
							Operand2: &basicExpression{
								Name: "1",
							},
							Conjunction: "",
						},
					},
				},
				EmitChanges: true,
			},
			wantErr: false,
		},
		// create stream with as select with single column and left join and where and partition by
		{
			name: "create stream with as select with single column and left join and where",
			args: args{
				sqls: "CREATE STREAM stream AS SELECT column FROM tbl LEFT JOIN tbl2 on tbl.field=tbl2.field " +
					"WHERE tbl.field=1 PARTITION BY tbl.field EMIT CHANGES;",
			},
			want: &createStreamStmt{
				stmt: stmt{
					Type: "CREATE",
					Name: "stream",
				},
				Select: &streamSelect{
					Expressions: []*aliasedExpression{
						{
							Expression: &basicExpression{
								Name: "column",
							},
							Alias: "",
						},
					},
					Identifier: identifier{
						Name:  "tbl",
						Alias: "",
					},
					Joins: &[]joinExpression{
						{
							Identifier: identifier{
								Name:  "tbl2",
								Alias: "",
							},
							Conditions: []*Condition{
								{
									Operand1: &basicExpression{
										Name: "tbl.field",
									},
									Operator: ReservedEq,
									Operand2: &basicExpression{
										Name: "tbl2.field",
									},
									Conjunction: "",
								},
							},
						},
					},
					Where: &[]*Condition{
						{
							Operand1: &basicExpression{
								Name: "tbl.field",
							},
							Operator: "=",
							Operand2: &basicExpression{
								Name: "1",
							},
							Conjunction: "",
						},
					},
					Partition: "tbl.field",
				},
				EmitChanges: true,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.debug {
				//runtime.Breakpoint()
			}
			got, err := Parse(tt.args.sqls)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isIdentifierRune(t *testing.T) {
	type args struct {
		c rune
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Space is not a identifier rune",
			args: args{
				c: ' ',
			},
			want: false,
		},
		{
			name: "a is a identifier rune",
			args: args{
				c: 'a',
			},
			want: true,
		},
		{
			name: "z is a identifier rune",
			args: args{
				c: 'z',
			},
			want: true,
		},
		{
			name: "A is a identifier rune",
			args: args{
				c: 'A',
			},
			want: true,
		},
		{
			name: "Z is a identifier rune",
			args: args{
				c: 'Z',
			},
			want: true,
		},
		{
			name: "0 is a identifier rune",
			args: args{
				c: '0',
			},
			want: true,
		},
		{
			name: "9 is a identifier rune",
			args: args{
				c: '9',
			},
			want: true,
		},
		{
			name: ". is a identifier rune",
			args: args{
				c: '.',
			},
			want: true,
		},
		{
			name: "= is not a identifier rune",
			args: args{
				c: '=',
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isIdentifierRune(tt.args.c); got != tt.want {
				t.Errorf("isIdentifierRune() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parser_peekIdentifierWithLength(t *testing.T) {
	type fields struct {
		i    int
		sql  string
		err  error
		line int
		col  int
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  int
	}{
		{
			name: "Basic test",
			fields: fields{
				i:   0,
				sql: "something here",
				err: nil,
			},
			want:  "something",
			want1: 9,
		},
		{
			name: "Basic test",
			fields: fields{
				i:   10,
				sql: "something here not here",
				err: nil,
			},
			want:  "here",
			want1: 4,
		},
		{
			name: "Basic test",
			fields: fields{
				i:   0,
				sql: "something.here not here",
				err: nil,
			},
			want:  "something.here",
			want1: 14,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &parser{
				i:    tt.fields.i,
				sql:  tt.fields.sql,
				line: tt.fields.line,
				col:  tt.fields.col,
			}
			got, got1 := p.peekIdentifierWithLength()
			if got != tt.want {
				t.Errorf("peekIdentifierWithLength() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("peekIdentifierWithLength() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_parser_popOrError(t *testing.T) {
	type fields struct {
		i    int
		sql  string
		line int
		col  int
	}
	type args struct {
		reservedWords []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "When parsing by order",
			fields: fields{
				i:    0,
				sql:  "HELLOS",
				line: 0,
				col:  0,
			},
			args: args{
				reservedWords: []string{
					"HELLOS",
					"HELLO",
					"EL",
					"LO",
				},
			},
			want:    "HELLOS",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &parser{
				i:    tt.fields.i,
				sql:  tt.fields.sql,
				line: tt.fields.line,
				col:  tt.fields.col,
			}
			got, err := p.popOrError(tt.args.reservedWords...)
			if (err != nil) != tt.wantErr {
				t.Errorf("popOrError() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("popOrError() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parser_pop(t *testing.T) {
	type fields struct {
		i    int
		sql  string
		line int
		col  int
	}
	type args struct {
		reservedWords []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "When parsing object->paths",
			fields: fields{
				i:    0,
				sql:  "order->total_basket hello",
				line: 0,
				col:  0,
			},
			args: args{
				reservedWords: []string{},
			},
			want: "order->total_basket",
		},
		{
			name: "When parsing 'identifier'",
			fields: fields{
				i:    0,
				sql:  "'identifier' something",
				line: 0,
				col:  0,
			},
			args: args{
				reservedWords: []string{},
			},
			want: "'identifier'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &parser{
				i:    tt.fields.i,
				sql:  tt.fields.sql,
				line: tt.fields.line,
				col:  tt.fields.col,
			}
			if got := p.pop(tt.args.reservedWords...); got != tt.want {
				t.Errorf("pop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parser_popWhitespace(t *testing.T) {
	type fields struct {
		i    int
		sql  string
		line int
		col  int
	}
	type want struct {
		i    int
		line int
		col  int
	}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "when no whitespace",
			fields: fields{
				i:    0,
				sql:  "nowhitespace",
				line: 0,
				col:  0,
			},
			want: want{
				i:    0,
				line: 0,
				col:  0,
			},
		},
		{
			name: "when whitespace",
			fields: fields{
				i:    0,
				sql:  "  nowhitespace",
				line: 0,
				col:  0,
			},
			want: want{
				i:    2,
				line: 0,
				col:  2,
			},
		},
		{
			name: "when carriage returns",
			fields: fields{
				i:    0,
				sql:  "  \n here",
				line: 0,
				col:  0,
			},
			want: want{
				i:    4,
				line: 1,
				col:  1,
			},
		},
		{
			name: "when inline comment",
			fields: fields{
				i:    0,
				sql:  " --here\nthere",
				line: 0,
				col:  0,
			},
			want: want{
				i:    8,
				line: 2,
				col:  0,
			},
		},
		{
			name: "when multiline comment",
			fields: fields{
				i:    0,
				sql:  " /*\nhere\nthere\n*/here",
				line: 0,
				col:  0,
			},
			want: want{
				i:    17,
				line: 3,
				col:  2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &parser{
				i:    tt.fields.i,
				sql:  tt.fields.sql,
				line: tt.fields.line,
				col:  tt.fields.col,
			}
			p.popWhitespace()
			got := want{
				i:    p.i,
				line: p.line,
				col:  p.col,
			}
			if got != tt.want {
				t.Errorf("popWhitespace() = %v, want %v", got, tt.want)
			}
		})
	}
}
