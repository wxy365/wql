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
	setNamespaceImplicit(ns string)
}

type namespaceAwareImpl struct {
	ns       string
	explicit bool
}

func (n *namespaceAwareImpl) namespace() opt.Opt[string] {
	return opt.Of(n.ns)
}

func (n *namespaceAwareImpl) setNamespace(ns string) {
	n.ns = ns
	n.explicit = true
}

// setNamespaceImplicit sets the namespace unless it has been set explicitly,
// which keeps a column from being qualified with the table of a previous query
// when the very same column is reused.
func (n *namespaceAwareImpl) setNamespaceImplicit(ns string) {
	if n.explicit {
		return
	}
	n.ns = ns
}
