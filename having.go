package q

import (
	"github.com/wxy365/basal/ds/maps"
	"github.com/wxy365/basal/text"
)

type HavingMdl struct {
	criterion   Criterion
	subCriteria []*AndOrCriteria
}

func (h *HavingMdl) GetStatement(ctx *RenderCtx) *Statement {
	params := make(map[int]any, 0)
	b := text.Build("having ")
	stm := h.criterion.GetStatement(ctx)
	b.Push(stm.stm)
	params = maps.Merge(params, stm.params)

	for _, s := range h.subCriteria {
		stm = s.GetStatement(ctx)
		b.Push(" ", stm.stm)
		params = maps.Merge(params, stm.params)
	}

	return &Statement{
		stm:    b.String(),
		params: params,
	}
}
