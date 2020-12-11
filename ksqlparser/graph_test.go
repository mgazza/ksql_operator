package ksqlparser

import (
	"github.com/go-test/deep"
	"testing"
)

func Test_buildDependencyGraph(t *testing.T) {

	stmts, err := Parse(ksql)
	if err != nil {
		t.Error(err)
	}

	want := []string{
		"ATTRIBUTIONS_CONFIG_TB",
		"ATTRIBUTIONS_ST",
		"EMAIL_ATTRIBUTIONS_ST",
		"INVOICES_ST",
		"PAGE_EVENT_2_ST",
		"PROMOCODE_ATTRIBUTIONS_ST",
		"L0orFlaTlO_YV908JLFGuHeIQ78fw7P4ESJRe3VusLM=",
		"REPORTING_TB",
		"sessions_engagements_displays_count",
		"SESSION_ACTIONS_ST",
		"DA_ATTRIBUTIONS_TB",
		"PROMOCODE_ATTRIBUTIONS_TB",
		"SESSION_ACTIONS_EXPLODED_ST",
		"HC_1MINUTE_TB",
		"SESSION_ACTIONS_V2_TB",
	}

	g := BuildDependencyGraph(stmts)

	var queryResult string
	var got []string
	for _, i := range g {
		got = append(got, i.GetName())

		queryResult = queryResult + "\n" + i.String()
	}

	if diff := deep.Equal(got, want); diff != nil {
		t.Errorf("BuildDependencyGraph() want = %v, got %v", want, got)
	}
}
