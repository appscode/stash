package controller

import (
	acrt "github.com/appscode/go/runtime"
	"github.com/appscode/log"
	sapi "github.com/appscode/stash/api"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	apiv1 "k8s.io/client-go/pkg/api/v1"
	extensions "k8s.io/client-go/pkg/apis/extensions/v1beta1"
	apps "k8s.io/client-go/pkg/apis/apps/v1beta1"
	"k8s.io/client-go/tools/cache"
)

// Blocks caller. Intended to be called as a Go routine.
func (c *Controller) WatchDeploymentApps() {
	defer acrt.HandleCrash()

	lw := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			return c.KubeClient.AppsV1beta1().Deployments(apiv1.NamespaceAll).List(metav1.ListOptions{})
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.KubeClient.AppsV1beta1().Deployments(apiv1.NamespaceAll).Watch(metav1.ListOptions{})
		},
	}
	_, ctrl := cache.NewInformer(lw,
		&apps.Deployment{},
		c.SyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				if rs, ok := obj.(*apps.Deployment); ok {
					log.Infof("Deployment %s@%s added", rs.Name, rs.Namespace)

					if name := getString(rs.Annotations, sapi.ConfigName); name != "" {
						log.Infof("Restic sidecar already exists for Deployment %s@%s.", rs.Name, rs.Namespace)
						return
					}

					restic, err := c.FindRestic(rs.ObjectMeta)
					if err != nil {
						log.Errorf("Error while searching Restic for Deployment %s@%s.", rs.Name, rs.Namespace)
						return
					}
					if restic != nil {
						log.Errorf("No Restic found for Deployment %s@%s.", rs.Name, rs.Namespace)
						return
					}

					rs.Spec.Template.Spec.Containers = append(rs.Spec.Template.Spec.Containers, c.GetSidecarContainer(restic))
					rs.Spec.Template.Spec.Volumes = append(rs.Spec.Template.Spec.Volumes, restic.Spec.Destination.Volume)
					if rs.Annotations == nil {
						rs.Annotations = make(map[string]string)
					}
					rs.Annotations[sapi.ConfigName] = restic.Name
					rs.Annotations[sapi.VersionTag] = c.SidecarImageTag

					rs, err = c.KubeClient.AppsV1beta1().Deployments(rs.Namespace).Update(rs)
					if kerr.IsNotFound(err) {
						return
					} else if err != nil {
						sidecarFailedToAdd()
						log.Errorf("Failed to add sidecar for Deployment %s@%s.", rs.Name, rs.Namespace)
						return
					}
					sidecarSuccessfullyAdd()
					c.restartPods(rs.Namespace, rs.Spec.Selector)
				}
			},
		},
	)
	ctrl.Run(wait.NeverStop)
}
