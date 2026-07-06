package q

import "strings"

// Col creates a Column from a name string. The name may include a namespace
// prefix separated by "." (e.g. "u.name") and/or an alias separated by a
// space (e.g. "u.name full_name").
func Col(name string) *Column {
	nameAlias := strings.Split(name, " ")
	res := &Column{}
	if len(nameAlias) > 0 {
		nsColName := strings.Split(nameAlias[0], ".")
		switch len(nsColName) {
		case 0:
		case 1:
			res.name = nsColName[0]
		default:
			res.setNamespace(nsColName[0])
			res.name = nsColName[1]
		}
		if len(nameAlias) > 1 {
			res.As(nameAlias[1])
		}
	}
	return res
}
