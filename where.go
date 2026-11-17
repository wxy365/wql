package q

import (
	"github.com/wxy365/basal/ds/maps"
	"github.com/wxy365/basal/text"
)

type WhereMdl struct {
	criterion   Criterion
	subCriteria []*AndOrCriteria
}

func (w *WhereMdl) GetStatement(ctx *RenderCtx) *Statement {
	params := make(map[int]any)
	if w.criterion == nil && len(w.subCriteria) == 0 {
		// Nothing to filter on, the WHERE clause is left out altogether.
		return &Statement{
			stm:    "",
			params: params,
		}
	}
	b := text.Build("where ")
	if w.criterion != nil {
		stm := w.criterion.GetStatement(ctx)
		b.Push(stm.stm)
		params = maps.Merge(params, stm.params)
	}

	for _, c := range w.subCriteria {
		stm := c.GetStatement(ctx)
		b.Push(" ", stm.stm)
		params = maps.Merge(params, stm.params)
	}

	return &Statement{
		stm:    b.String(),
		params: params,
	}
}
