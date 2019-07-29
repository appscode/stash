package controller

import (
	"fmt"
	"strings"

	"github.com/golang/glog"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/reference"
	meta_util "kmodules.xyz/client-go/meta"
	"kmodules.xyz/client-go/tools/queue"
	appCatalog "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	catalog_scheme "kmodules.xyz/custom-resources/client/clientset/versioned/scheme"
	"stash.appscode.dev/stash/apis"
	api_v1beta1 "stash.appscode.dev/stash/apis/stash/v1beta1"
	"stash.appscode.dev/stash/pkg/resolve"
)

func (c *StashController) initAppBindingWatcher() {
	c.abInformer = c.appCatalogInformerFactory.Appcatalog().V1alpha1().AppBindings().Informer()
	c.abQueue = queue.New(apis.KindAppBinding, c.MaxNumRequeues, c.NumThreads, c.processAppBindingKey)
	c.abInformer.AddEventHandler(queue.DefaultEventHandler(c.abQueue.GetQueue()))
	c.abLister = c.appCatalogInformerFactory.Appcatalog().V1alpha1().AppBindings().Lister()
}

// syncToStdout is the business logic of the controller. In this controller it simply prints
// information about the deployment to stdout. In case an error happened, it has to simply return the error.
// The retry logic should not be part of the business logic.
func (c *StashController) processAppBindingKey(key string) error {
	obj, exists, err := c.abInformer.GetIndexer().GetByKey(key)
	if err != nil {
		glog.Errorf("Fetching object with key %s from store failed with %v", key, err)
		return err
	}

	if !exists {
		// Below we will warm up our cache with a AppBinding, so that we will see a delete for one databases.
		glog.Warningf("AppBinding %s does not exist anymore\n", key)
	} else {
		glog.Infof("Sync/Add/Update for AppBinding %s", key)

		ab := obj.(*appCatalog.AppBinding).DeepCopy()
		ab.GetObjectKind().SetGroupVersionKind(core.SchemeGroupVersion.WithKind(apis.KindAppBinding))

		err := c.applyBackupAnnotationLogicForAppBinding(ab)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *StashController) applyBackupAnnotationLogicForAppBinding(ab *appCatalog.AppBinding) error {
	targetRef, err := reference.GetReference(catalog_scheme.Scheme, ab)
	if err != nil {
		return fmt.Errorf("failed to create reference of %s %s/%s. Reason: %v", ab.Kind, ab.Namespace, ab.Name, err)
	}

	// if ab has backup annotations then ensure respective Repository and BackupConfiguration
	if meta_util.HasKey(ab.Annotations, api_v1beta1.KeyBackupBlueprint) {
		// backup annotations found. so, we have to ensure Repository and BackupConfiguration from BackupBlueprint
		backupBlueprintName, err := meta_util.GetStringValue(ab.Annotations, api_v1beta1.KeyBackupBlueprint)
		if err != nil {
			return err
		}

		backupBlueprint, err := c.stashClient.StashV1beta1().BackupBlueprints().Get(backupBlueprintName, metav1.GetOptions{})
		if err != nil {
			return err
		}

		// resolve BackupBlueprint's variables
		inputs := make(map[string]string)
		inputs[apis.TargetAPIVersion] = ab.APIVersion
		inputs[apis.TargetKind] = strings.ToLower(ab.Kind)
		inputs[apis.TargetName] = ab.Name
		inputs[apis.TargetNamespace] = ab.Namespace
		inputs[apis.TargetAppVersion] = ab.Spec.Version

		err = resolve.ResolveBackupBlueprint(backupBlueprint, inputs)
		if err != nil {
			return err
		}

		// ensure Repository crd
		err = c.ensureRepository(backupBlueprint, targetRef)
		if err != nil {
			return err
		}

		// ensure BackupConfiguration crd
		err = c.ensureBackupConfiguration(backupBlueprint, nil, nil, targetRef)
		if err != nil {
			return err
		}

	} else {
		// app binding does not have backup annotations. it might be removed or was never added.
		// if respective BackupConfiguration exist then backup annotations has been removed.
		// in this case, we have to remove the BackupConfiguration too.
		// however, we will keep Repository crd as it is required for restore.
		_, err := c.stashClient.StashV1beta1().BackupConfigurations(ab.Namespace).Get(getBackupConfigurationName(targetRef), metav1.GetOptions{})
		if err != nil && !kerr.IsNotFound(err) {
			return err
		}
		// BackupConfiguration exist. so, we have to remove it.
		err = c.stashClient.StashV1beta1().BackupConfigurations(ab.Namespace).Delete(getBackupConfigurationName(targetRef), meta_util.DeleteInBackground())
		if err != nil && !kerr.IsNotFound(err) {
			return err
		}
	}
	return nil
}
