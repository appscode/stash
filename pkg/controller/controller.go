package controller

import (
	"time"

	sapi "github.com/appscode/stash/api"
	scs "github.com/appscode/stash/client/clientset"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	extensions "k8s.io/client-go/pkg/apis/extensions/v1beta1"
)

type Controller struct {
	kubeClient      clientset.Interface
	stashClient     scs.ExtensionInterface
	SidecarImageTag string
	syncPeriod      time.Duration
}

func New(kubeClient clientset.Interface, extClient scs.ExtensionInterface, tag string) *Controller {
	return &Controller{
		kubeClient:      kubeClient,
		stashClient:     extClient,
		SidecarImageTag: tag,
		syncPeriod:      30 * time.Second,
	}
}

func (c *Controller) Setup() error {
	_, err := c.kubeClient.ExtensionsV1beta1().ThirdPartyResources().Get(scs.ResourceNameRestic+"."+sapi.GroupName, metav1.GetOptions{})
	if kerr.IsNotFound(err) {
		tpr := &extensions.ThirdPartyResource{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "extensions/v1beta1",
				Kind:       "ThirdPartyResource",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: scs.ResourceNameRestic + "." + sapi.GroupName,
			},
			Versions: []extensions.APIVersion{
				{
					Name: "v1alpha1",
				},
			},
		}
		_, err := c.kubeClient.ExtensionsV1beta1().ThirdPartyResources().Create(tpr)
		if err != nil {
			// This should fail if there is one third party resource data missing.
			return err
		}
	}
	return nil
}

func (c *Controller) Run() {
	go c.WatchNamespaces()
	go c.WatchRestics()
	go c.WatchDaemonSets()
	go c.WatchDeploymentApps()
	go c.WatchDeploymentExtensions()
	go c.WatchReplicaSets()
	go c.WatchReplicationControllers()
}
