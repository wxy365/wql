package q

import "github.com/wxy365/basal/opt"

type operational interface {
	operator() string
}

type operationalImpl struct {
	op string
}

func (o *operationalImpl) operator() string {
	return o.op
}

type aliasable[T any] interface {
	Alias() opt.Opt[string]
	As(string) T
}

type named interface {
	Name() string
}

type namedImpl struct {
	name string
}

func (n *namedImpl) Name() string {
	return n.name
}

type orderAware interface {
	isDesc() bool
}

type namespaceAware interface {
	namespace() opt.Opt[string]
	setNamespace(ns string)
}

type namespaceAwareImpl struct {
	ns string
}

func (n *namespaceAwareImpl) namespace() opt.Opt[string] {
	return opt.Of(n.ns)
}

func (n *namespaceAwareImpl) setNamespace(ns string) {
	n.ns = ns
}
