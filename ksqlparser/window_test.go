package ksqlparser

import (
	"github.com/go-test/deep"
	"testing"
)

func Test_parser_parseWindow(t *testing.T) {
	type fields struct {
		i    int
		sql  string
		line int
		col  int
	}
	tests := []struct {
		name    string
		fields  fields
		want    *WindowExpression
		wantErr bool
	}{
		{
			name: "When parsing HOPPING (SIZE 1 minute, ADVANCE BY 30 SECONDS, RETENTION 2 MINUTES, GRACE PERIOD 0 SECONDS)",
			fields: fields{
				i:    0,
				sql:  "HOPPING (SIZE 1 minute, ADVANCE BY 30 SECONDS, RETENTION 2 MINUTES, GRACE PERIOD 0 SECONDS)",
				line: 0,
				col:  0,
			},
			want: &WindowExpression{
				Type:            WindowTypeHopping,
				Size:            1,
				SizeType:        WindowTimePeriodMinute,
				Advance:         30,
				AdvanceType:     WindowTimePeriodSeconds,
				Retention:       2,
				RetentionType:   WindowTimePeriodMinutes,
				GracePeriod:     0,
				GracePeriodType: WindowTimePeriodSeconds,
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
			got, err := p.parseWindow()
			if (err != nil) != tt.wantErr {
				t.Errorf("parseWindow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Errorf("parseWindow() got = %v, want %v", got, tt.want)
			}
		})
	}
}
