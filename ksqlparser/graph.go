package ksqlparser

import (
	"sort"
	"strings"
)

type graphitem struct {
	name       string
	stmt       Stmt
	depends    map[string]graphitem
	dependants map[string]graphitem
}

type dependencyGraph []graphitem

func (a dependencyGraph) Len() int           { return len(a) }
func (a dependencyGraph) Less(i, j int) bool { return a[i].name < a[j].name }
func (a dependencyGraph) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func BuildDependencyGraph(stmts []Stmt) []Stmt {
	stmtMap := map[string]graphitem{}
	for _, item := range stmts {
		n := strings.ToUpper(item.GetName())
		stmtMap[n] = graphitem{
			name:       n,
			stmt:       item,
			depends:    map[string]graphitem{},
			dependants: map[string]graphitem{},
		}
	}

	// build the graphitem
	var stmtDeps []graphitem
	for _, stmtDep := range stmtMap {
		dataSources := stmtDep.stmt.GetDataSources()
		for _, dataSource := range dataSources {
			dependantStmtDep, ok := stmtMap[strings.ToUpper(dataSource)]
			if ok {
				stmtDep.depends[dependantStmtDep.name] = dependantStmtDep
				dependantStmtDep.dependants[stmtDep.name] = stmtDep
			}
		}
		stmtDeps = append(stmtDeps, stmtDep)
	}

	// make the result deterministic for testing
	sort.Sort(dependencyGraph(stmtDeps))

	var order []Stmt
	for i := 0; len(stmtDeps) > 0; i++ {
		item := stmtDeps[i]
		if len(item.depends) == 0 {
			stmtDeps = append(stmtDeps[:i], stmtDeps[i+1:]...)
			// this is a root (or now is a root)
			order = append(order, item.stmt)
			// remove us as a blocker from our dependants
			for _, dep := range item.dependants {
				delete(dep.depends, item.name)
			}
			i = -1
		}
	}
	return order
}
