package ksqlparser

import (
	"github.com/go-test/deep"
	"testing"
)

func Test_parser_parseExpression(t *testing.T) {
	type fields struct {
		i    int
		sql  string
		line int
		col  int
	}
	tests := []struct {
		name    string
		fields  fields
		want    Expression
		wantErr bool
	}{
		/*
			// When parsing CASE WHEN 1 = 2 THEN 1 ELSE 2 END
			{
				name: "When parsing CASE WHEN 1 = 2 THEN 1 ELSE 2 END",
				fields: fields{
					i:    0,
					sql:  "CASE WHEN 1 = 2 THEN 1 ELSE 2 END",
					line: 0,
					col:  0,
				},
				want: &caseWhenExpression{
					When: []Condition{
						{
							Operand1: &basicExpression{
								Name: "1",
							},
							Operator: "=",
							Operand2: &basicExpression{
								Name: "2",
							},
							Conjunction: "",
						},
					},
					Then: &basicExpression{
						Name: "1",
					},
					Else: &basicExpression{
						Name: "2",
					},
				},
				wantErr: false,
			},
			// When parsing CASE WHEN foo like bar THEN foo ELSE bar END
			{
				name: "When parsing CASE WHEN foo like bar THEN foo ELSE bar END",
				fields: fields{
					i:    0,
					sql:  "CASE WHEN foo like bar THEN foo ELSE bar END",
					line: 0,
					col:  0,
				},
				want: &caseWhenExpression{
					When: []Condition{
						{
							Operand1: &basicExpression{
								Name: "foo",
							},
							Operator: "LIKE",
							Operand2: &basicExpression{
								Name: "bar",
							},
							Conjunction: "",
						},
					},
					Then: &basicExpression{
						Name: "foo",
					},
					Else: &basicExpression{
						Name: "bar",
					},
				},
				wantErr: false,
			},
			// When parsing AS_MAP(collect_list(CAST(timestamp AS STRING)), collect_list(field))
			{
				name: "When parsing AS_MAP(collect_list(CAST(timestamp AS STRING)), collect_list(field))",
				fields: fields{
					i:    0,
					sql:  "AS_MAP(collect_list(CAST(timestamp AS STRING)), collect_list(field))",
					line: 0,
					col:  0,
				},
				want: &functionExpression{
					Name: "AS_MAP",
					Params: []Expression{
						&functionExpression{
							Name: "COLLECT_LIST",
							Params: []Expression{
								&castExpression{
									InnerExpression: &basicExpression{
										Name: "timestamp",
									},
									DataType: &simpleDataType{
										Type: DataTypeString,
									},
								},
							},
						},
						&functionExpression{
							Name: "COLLECT_LIST",
							Params: []Expression{
								&basicExpression{
									Name: "field",
								},
							},
						},
					},
				},
				wantErr: false,
			},
			// When parsing AS_MAP(field , field)
			{
				name: "When parsing AS_MAP(field , field)",
				fields: fields{
					i:    0,
					sql:  "AS_MAP(field , field)",
					line: 0,
					col:  0,
				},
				want: &functionExpression{
					Name: "AS_MAP",
					Params: []Expression{
						&basicExpression{
							Name: "field",
						},
						&basicExpression{
							Name: "field",
						},
					},
				},
				wantErr: false,
			},
			// When parsing CAST('1' AS STRING)
			{
				name: "When parsing CAST('1' AS STRING)",
				fields: fields{
					i:    0,
					sql:  "CAST(1 AS STRING)",
					line: 0,
					col:  0,
				},
				want: &castExpression{
					InnerExpression: &basicExpression{
						Name: "1",
					},
					DataType: &simpleDataType{
						Type: DataTypeString,
					},
				},
				wantErr: false,
			},
			// When parsing 1+1+1
			{
				name: "When parsing 1 + 1 + 1",
				fields: fields{
					i:    0,
					sql:  "1 + 1 + 1",
					line: 0,
					col:  0,
				},
				want: &operatorExpression{
					LeftExpression:  &basicExpression{
						Name: "1",
					},
					RightExpression: &operatorExpression{
						LeftExpression: &basicExpression{
							Name: "1",
						},
						RightExpression: &basicExpression{
							Name: "1",
						},
						Operator: "+",
					},
					Operator: "+",
				},
				wantErr: false,
			},
		*/
		// When parsing multiple stmt[index]+stmt[index]
		{
			name: "When parsing stmt[index]+stmt[index]",
			fields: fields{
				i:    0,
				sql:  "stmt[index]+stmt[index]",
				line: 0,
				col:  0,
			},
			want: &operatorExpression{
				LeftExpression: &indexExpression{
					Expression: &basicExpression{
						Name: "stmt",
					},
					Index: &basicExpression{
						Name: "index",
					},
				},
				RightExpression: &indexExpression{
					Expression: &basicExpression{
						Name: "stmt",
					},
					Index: &basicExpression{
						Name: "index",
					},
				},
				Operator: "+",
			},
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
			got, err := p.parseExpression()
			if (err != nil) != tt.wantErr {
				t.Errorf("parseExpression() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Errorf("parseExpression() got = %v, want %v", got, tt.want)
			}
		})
	}
}
