package ksqlclient

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"ksql_operator/ksqlclient/swagger"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/assert"
)

// RoundTripFunc .
type RoundTripFunc func(req *http.Request) *http.Response

// RoundTrip .
func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func NewRoundTrip(fn RoundTripFunc) http.RoundTripper {
	return RoundTripFunc(fn)
}

func Test_client_Describe(t *testing.T) {

	type response struct {
		code int
		body string
	}
	type args struct {
		ctx      context.Context
		name     string
		response response
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "describe exists",
			args: args{
				ctx:  nil,
				name: "SESSION_ACTIONS_ST",
				response: response{
					code: 200,
					body: `
[{
    "@type": "sourceDescription",
    "statementText": "DESCRIBE SESSION_ACTIONS_ST;",
    "sourceDescription": {
      "name": "SESSION_ACTIONS_ST",
      "windowType": null,
      "readQueries": [],
      "writeQueries": [],
      "fields": [
        {
          "name": "ACTIONS",
          "schema": {
            "type": "ARRAY",
            "fields": null,
            "memberSchema": {
              "type": "STRING",
              "fields": null,
              "memberSchema": null
            }
          }
        }
      ],
      "type": "STREAM",
      "timestamp": "",
      "statistics": "",
      "errorStats": "",
      "extended": false,
      "keyFormat": "KAFKA",
      "valueFormat": "JSON",
      "topic": "SESSION_ACTIONS_V2",
      "partitions": 0,
      "replication": 0,
      "statement": "CREATE STREAM SESSION_ACTIONS_ST (\nactions ARRAY<STRING>) \nWITH (\nKAFKA_TOPIC = 'SESSION_ACTIONS_V2',\nPARTITIONS = 2,\nREPLICAS = 1,\nVALUE_FORMAT = 'JSON'\n);"
    },
    "warnings": []
  }
]`,
				},
			},
			want: &[]swagger.DescribeResultItem{
				{
					Type_:         "sourceDescription",
					StatementText: "DESCRIBE SESSION_ACTIONS_ST;",
					SourceDescription: &swagger.DescribeResultItemSourceDescription{
						Name:         "SESSION_ACTIONS_ST",
						WindowType:   "",
						ReadQueries:  []interface{}{},
						WriteQueries: []interface{}{},
						Fields: []swagger.DescribeResultItemSourceDescriptionFields{
							{
								Name: "ACTIONS",
								Schema: &swagger.DescribeResultItemSourceDescriptionSchema{
									Type_: "ARRAY",
									MemberSchema: map[string]interface{}{
										"type":         "STRING",
										"fields":       nil,
										"memberSchema": nil,
									},
									Fields: nil,
								},
							},
						},
						Type_:       "STREAM",
						Key:         "",
						Timestamp:   "",
						Format:      nil,
						Topic:       "SESSION_ACTIONS_V2",
						Extended:    false,
						Statistics:  "",
						ErrorStats:  "",
						Replication: 0,
						Partitions:  0,
						Statement:   "CREATE STREAM SESSION_ACTIONS_ST (\nactions ARRAY<STRING>) \nWITH (\nKAFKA_TOPIC = 'SESSION_ACTIONS_V2',\nPARTITIONS = 2,\nREPLICAS = 1,\nVALUE_FORMAT = 'JSON'\n);",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Monkey patch the transport for http
			oldtransport := http.DefaultTransport
			defer func() {
				http.DefaultTransport = oldtransport
			}()
			http.DefaultTransport = NewRoundTrip(
				func(req *http.Request) *http.Response {
					assert.Equal(t, "ksql", req.URL)
					return &http.Response{
						StatusCode: tt.args.response.code,
						Body:       ioutil.NopCloser(bytes.NewBufferString(tt.args.response.body)),
						Header:     make(http.Header),
					}

				},
			)

			k := client{}
			got, err := k.Describe(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Describe() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Errorf("Describe() got = %v, want %v, diff=%v", got, tt.want, diff)
			}
		})
	}
}

func Test_client_Execute(t *testing.T) {
	type args struct {
		ctx    context.Context
		ksql   string
		result interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := client{}
			got, err := k.Execute(tt.args.ctx, tt.args.ksql, tt.args.result)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Execute() got = %v, want %v", got, tt.want)
			}
		})
	}
}
