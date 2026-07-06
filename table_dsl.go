package q

import "strings"

// Tbl creates a Table from a name string. The name may include an alias
// separated by a space (e.g. "user u").
func Tbl(name string) *Table {
	res := &Table{}
	nameAlias := strings.Split(name, " ")
	if len(nameAlias) > 0 {
		res.name = nameAlias[0]
		if len(nameAlias) > 1 {
			res.As(nameAlias[1])
		}
	}
	return res
}
