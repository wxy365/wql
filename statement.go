package q

import (
	"strconv"
	"strings"
)

type Statement struct {
	stm    string
	params map[int]any
}

func (s *Statement) prepare() (string, []any) {
	var b strings.Builder
	var params []any
	r := []rune(s.stm)
	for i := 0; i < len(r); i++ {
		c := r[i]
		switch c {
		case ':':
			b.WriteRune('?')
			paramNameEnd := strings.IndexAny(string(r[i:]), " ,;)\n")
			if paramNameEnd < 0 {
				paramNameEnd = len(r)
			}
			paramName := r[i+1 : i+paramNameEnd]
			paramNameInt, _ := strconv.Atoi(string(paramName))
			params = append(params, s.params[paramNameInt])
			i += paramNameEnd
		default:
			b.WriteRune(c)
		}
	}
	return b.String(), params
}

type StatementProvider interface {
	GetStatement(ctx *RenderCtx) *Statement
}
