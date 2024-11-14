package q

import "github.com/wxy365/basal/fn"

type Builder[T any] interface {
	Build() T
}

type RenderCtx struct {
	dbType DbType
	cnt    *fn.Counter
}
