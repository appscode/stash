package controller

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	rt "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	apiv1 "k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"
)

func (c *StashController) initNamespaceWatcher() {
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (rt.Object, error) {
			return c.k8sClient.CoreV1().Namespaces().List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.k8sClient.CoreV1().Namespaces().Watch(options)
		},
	}

	c.nsIndexer, c.nsInformer = cache.NewIndexerInformer(lw, &apiv1.Namespace{}, c.options.ResyncPeriod, cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			if ns, ok := obj.(*apiv1.Namespace); ok {
				restics, err := c.rstLister.Restics(ns.Name).List(labels.Everything())
				if err == nil {
					for _, restic := range restics {
						c.stashClient.Restics(restic.Namespace).Delete(restic.Name, &metav1.DeleteOptions{})
					}
				}
			}
		},
	}, cache.Indexers{})
}
