package ksqlparser

import (
	"github.com/go-test/deep"
	"testing"
)

func Test_parser_parseConditions(t *testing.T) {
	type fields struct {
		i    int
		sql  string
		line int
		col  int
	}
	tests := []struct {
		name    string
		fields  fields
		want    []*Condition
		wantErr bool
	}{
		// when parsing COUNT(session_id + attribution_actions['purchase']) = 1
		{
			name: "when parsing COUNT(session_id + attribution_actions['purchase']) = 1",
			fields: fields{
				i:    0,
				sql:  "COUNT(session_id + attribution_actions['purchase']) = 1",
				line: 0,
				col:  0,
			},
			want: []*Condition{
				{
					Operand1: &functionExpression{
						Name: "COUNT",
						Params: []Expression{
							&operatorExpression{
								LeftExpression: &basicExpression{
									Name: "session_id",
								},
								Operator: "+",
								RightExpression: &indexExpression{
									Expression: &basicExpression{
										Name: "attribution_actions",
									},
									Index: &basicExpression{
										Name: "'purchase'",
									},
								},
							},
						},
					},
					Operator: "=",
					Operand2: &basicExpression{
						Name: "1",
					},
					Conjunction: "",
				},
			},
			wantErr: false,
		},
		//COUNT(session_id + attribution_actions['engaged_digital_assistant'] + attribution_actions['purchase']) = 1
		{
			name: "when parsing COUNT(session_id + attribution_actions['engaged_digital_assistant'] + attribution_actions['purchase']) = 1",
			fields: fields{
				i:    0,
				sql:  "COUNT(session_id + attribution_actions['engaged_digital_assistant'] + attribution_actions['purchase']) = 1",
				line: 0,
				col:  0,
			},
			want: []*Condition{
				{
					Operand1: &functionExpression{
						Name: "COUNT",
						Params: []Expression{
							&operatorExpression{
								LeftExpression: &basicExpression{
									Name: "session_id",
								},
								Operator: "+",
								RightExpression: &operatorExpression{
									LeftExpression: &indexExpression{
										Expression: &basicExpression{
											Name: "attribution_actions",
										},
										Index: &basicExpression{
											Name: "'engaged_digital_assistant'",
										},
									},
									Operator: "+",
									RightExpression: &indexExpression{
										Expression: &basicExpression{
											Name: "attribution_actions",
										},
										Index: &basicExpression{
											Name: "'purchase'",
										},
									},
								},
							},
						},
					},
					Operator: "=",
					Operand2: &basicExpression{
						Name: "1",
					},
					Conjunction: "",
				},
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
			got, err := p.parseConditions()
			if (err != nil) != tt.wantErr {
				t.Errorf("parseConditions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Errorf("parseConditions() got = %v, want %v", got, tt.want)
			}
		})
	}
}
