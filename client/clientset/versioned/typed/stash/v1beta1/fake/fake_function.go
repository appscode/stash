/*
Copyright 2019 The Stash Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	v1beta1 "github.com/appscode/stash/apis/stash/v1beta1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeFunctions implements FunctionInterface
type FakeFunctions struct {
	Fake *FakeStashV1beta1
}

var functionsResource = schema.GroupVersionResource{Group: "stash.appscode.com", Version: "v1beta1", Resource: "functions"}

var functionsKind = schema.GroupVersionKind{Group: "stash.appscode.com", Version: "v1beta1", Kind: "Function"}

// Get takes name of the function, and returns the corresponding function object, and an error if there is any.
func (c *FakeFunctions) Get(name string, options v1.GetOptions) (result *v1beta1.Function, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootGetAction(functionsResource, name), &v1beta1.Function{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.Function), err
}

// List takes label and field selectors, and returns the list of Functions that match those selectors.
func (c *FakeFunctions) List(opts v1.ListOptions) (result *v1beta1.FunctionList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootListAction(functionsResource, functionsKind, opts), &v1beta1.FunctionList{})
	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1beta1.FunctionList{ListMeta: obj.(*v1beta1.FunctionList).ListMeta}
	for _, item := range obj.(*v1beta1.FunctionList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested functions.
func (c *FakeFunctions) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewRootWatchAction(functionsResource, opts))
}

// Create takes the representation of a function and creates it.  Returns the server's representation of the function, and an error, if there is any.
func (c *FakeFunctions) Create(function *v1beta1.Function) (result *v1beta1.Function, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootCreateAction(functionsResource, function), &v1beta1.Function{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.Function), err
}

// Update takes the representation of a function and updates it. Returns the server's representation of the function, and an error, if there is any.
func (c *FakeFunctions) Update(function *v1beta1.Function) (result *v1beta1.Function, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootUpdateAction(functionsResource, function), &v1beta1.Function{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.Function), err
}

// Delete takes name of the function and deletes it. Returns an error if one occurs.
func (c *FakeFunctions) Delete(name string, options *v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewRootDeleteAction(functionsResource, name), &v1beta1.Function{})
	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeFunctions) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	action := testing.NewRootDeleteCollectionAction(functionsResource, listOptions)

	_, err := c.Fake.Invokes(action, &v1beta1.FunctionList{})
	return err
}

// Patch applies the patch and returns the patched function.
func (c *FakeFunctions) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1beta1.Function, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootPatchSubresourceAction(functionsResource, name, data, subresources...), &v1beta1.Function{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1beta1.Function), err
}
