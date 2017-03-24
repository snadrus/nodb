package base

import (
	"fmt"
	"sort"
	"strings"
)

type RowProvider interface {
	GetInfo() (multiPassCost int)
	SetConfig(multiPass bool) /* FUTURE: indexes */
	NextRow() (hasNotLooped bool)
	GetFields(used map[string]bool, addPrefix string, dest map[string]interface{})
}

type SrcTable struct {
	Table            RowProvider
	Fields           []string        // All public fields, real case
	UsedFields       map[string]bool // Fields actually consumed, real case
	Name             string
	HasPrivateFields bool // Cannot copy these. Query/filter/result cannot ref them.
}

type SrcTables map[string]*SrcTable

// ResolveRefAndMarkUsed takes a lowercased, potentially qualified string and finds it
// in the structs given, returning the actual parse tree to the value
func (ts *SrcTables) ResolveRefAndMarkUsed(ref string) (string, error) {
	Debug("resolve against table:", ts, "for", ref)
	pcs := strings.Split(ref, ".")
	//tables := (*ts).(map[string]SrcTable)
	if t, ok := (*ts)[pcs[0]]; len(pcs) > 1 && ok { // ts already lowercased by FROM
		properCasedString, e := t.resolveRef(pcs[1:])
		if e != nil {
			return "", e
		}
		if len(properCasedString) != 0 {
			return t.Name + "." + properCasedString, nil
		}
	}

	for _, k := range ts.sortedKeys() {
		t := (*ts)[k]
		if properCasedString, err := t.resolveRef(pcs); len(properCasedString) != 0 {
			return t.Name + "." + properCasedString, nil
		} else if err != nil {
			return "", err
		}
	}

	return "", fmt.Errorf("Column Ref %s not found.", ref)
}

// sortedKeys naturally uses 1select before other tables
func (ts *SrcTables) sortedKeys() (s []string) {
	for k, _ := range *ts {
		s = append(s, k)
	}
	sort.Strings(s)
	return
}

func (t *SrcTable) resolveRef(pcs []string) (string, error) {
	for _, f := range t.Fields {
		lowf := strings.ToLower(f)
		if lowf == pcs[0] {
			if len(pcs) > 1 {
				return "", fmt.Errorf("Ref into structs not impl, TODO")
			}
			t.UsedFields[f] = true
			return f, nil
		}
	}
	return "", nil
}
