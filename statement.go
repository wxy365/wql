package q

import (
	"strconv"
	"strings"
)

type Statement struct {
	stm    string
	params map[int]any
}

// prepare rewrites the named placeholders of the statement (:1, :2, ...) into
// the positional placeholders understood by the database driver (?), and
// returns the bound values in the order the placeholders appear.
//
// A ':' is only treated as the beginning of a placeholder when it is followed
// by digits naming a value that is actually bound to the statement. Any other
// ':' (a time literal such as '12:30', for instance) is kept verbatim.
func (s *Statement) prepare() (string, []any) {
	if s == nil {
		return "", nil
	}
	var b strings.Builder
	var params []any
	r := []rune(s.stm)
	for i := 0; i < len(r); i++ {
		c := r[i]
		if c != ':' {
			b.WriteRune(c)
			continue
		}
		nameEnd := i + 1
		for nameEnd < len(r) && r[nameEnd] >= '0' && r[nameEnd] <= '9' {
			nameEnd++
		}
		if nameEnd == i+1 {
			// No digits follow the ':', it is not a placeholder.
			b.WriteRune(c)
			continue
		}
		name, err := strconv.Atoi(string(r[i+1 : nameEnd]))
		if err != nil {
			b.WriteRune(c)
			continue
		}
		value, bound := s.params[name]
		if !bound {
			// Nothing is bound to that name, keep the text as it is.
			b.WriteRune(c)
			continue
		}
		b.WriteRune('?')
		params = append(params, value)
		i = nameEnd - 1
	}
	return b.String(), params
}

type StatementProvider interface {
	GetStatement(ctx *RenderCtx) *Statement
}
