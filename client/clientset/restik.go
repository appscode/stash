package clientset

import (
	rapi "github.com/appscode/stash/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

type StashNamespacer interface {
	Stashs(namespace string) StashInterface
}

const (
	ResourceKindStash = "Stash"
	ResourceNameStash = "stash"
	ResourceTypeStash = "stashs"
)

type StashInterface interface {
	List(opts metav1.ListOptions) (*rapi.StashList, error)
	Get(name string) (*rapi.Stash, error)
	Create(stash *rapi.Stash) (*rapi.Stash, error)
	Update(stash *rapi.Stash) (*rapi.Stash, error)
	Delete(name string, options *metav1.DeleteOptions) error
	Watch(opts metav1.ListOptions) (watch.Interface, error)
	UpdateStatus(stash *rapi.Stash) (*rapi.Stash, error)
}

type StashImpl struct {
	r  rest.Interface
	ns string
}

var _ StashInterface = &StashImpl{}

func newStash(c *ExtensionClient, namespace string) *StashImpl {
	return &StashImpl{c.restClient, namespace}
}

func (c *StashImpl) List(opts metav1.ListOptions) (result *rapi.StashList, err error) {
	result = &rapi.StashList{}
	err = c.r.Get().
		Namespace(c.ns).
		Resource(ResourceTypeStash).
		VersionedParams(&opts, ExtendedCodec).
		Do().
		Into(result)
	return
}

func (c *StashImpl) Get(name string) (result *rapi.Stash, err error) {
	result = &rapi.Stash{}
	err = c.r.Get().
		Namespace(c.ns).
		Resource(ResourceTypeStash).
		Name(name).
		Do().
		Into(result)
	return
}

func (c *StashImpl) Create(stash *rapi.Stash) (result *rapi.Stash, err error) {
	result = &rapi.Stash{}
	err = c.r.Post().
		Namespace(c.ns).
		Resource(ResourceTypeStash).
		Body(stash).
		Do().
		Into(result)
	return
}

func (c *StashImpl) Update(stash *rapi.Stash) (result *rapi.Stash, err error) {
	result = &rapi.Stash{}
	err = c.r.Put().
		Namespace(c.ns).
		Resource(ResourceTypeStash).
		Name(stash.Name).
		Body(stash).
		Do().
		Into(result)
	return
}

func (c *StashImpl) Delete(name string, options *metav1.DeleteOptions) (err error) {
	return c.r.Delete().
		Namespace(c.ns).
		Resource(ResourceTypeStash).
		Name(name).
		Body(options).
		Do().
		Error()
}

func (c *StashImpl) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return c.r.Get().
		Prefix("watch").
		Namespace(c.ns).
		Resource(ResourceTypeStash).
		VersionedParams(&opts, ExtendedCodec).
		Watch()
}

func (c *StashImpl) UpdateStatus(stash *rapi.Stash) (result *rapi.Stash, err error) {
	result = &rapi.Stash{}
	err = c.r.Put().
		Namespace(c.ns).
		Resource(ResourceTypeStash).
		Name(stash.Name).
		SubResource("status").
		Body(stash).
		Do().
		Into(result)
	return
}
