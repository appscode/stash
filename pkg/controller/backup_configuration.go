/*
Copyright The Stash Authors.

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

package controller

import (
	"fmt"
	"strings"

	"stash.appscode.dev/stash/apis"
	api_v1beta1 "stash.appscode.dev/stash/apis/stash/v1beta1"
	stash_scheme "stash.appscode.dev/stash/client/clientset/versioned/scheme"
	v1beta1_util "stash.appscode.dev/stash/client/clientset/versioned/typed/stash/v1beta1/util"
	"stash.appscode.dev/stash/pkg/docker"
	"stash.appscode.dev/stash/pkg/eventer"
	stash_rbac "stash.appscode.dev/stash/pkg/rbac"
	"stash.appscode.dev/stash/pkg/util"

	"github.com/appscode/go/log"
	"github.com/golang/glog"
	batch_v1beta1 "k8s.io/api/batch/v1beta1"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/reference"
	batch_util "kmodules.xyz/client-go/batch/v1beta1"
	core_util "kmodules.xyz/client-go/core/v1"
	"kmodules.xyz/client-go/tools/queue"
	v1 "kmodules.xyz/offshoot-api/api/v1"
	workload_api "kmodules.xyz/webhook-runtime/apis/workload/v1"
)

// TODO: Add validator that will reject to create BackupConfiguration if any Restic exist for target workload

func (c *StashController) initBackupConfigurationWatcher() {
	c.bcInformer = c.stashInformerFactory.Stash().V1beta1().BackupConfigurations().Informer()
	c.bcQueue = queue.New(api_v1beta1.ResourceKindBackupConfiguration, c.MaxNumRequeues, c.NumThreads, c.runBackupConfigurationProcessor)
	c.bcInformer.AddEventHandler(queue.NewReconcilableHandler(c.bcQueue.GetQueue()))
	c.bcLister = c.stashInformerFactory.Stash().V1beta1().BackupConfigurations().Lister()
}

// syncToStdout is the business logic of the controller. In this controller it simply prints
// information about the deployment to stdout. In case an error happened, it has to simply return the error.
// The retry logic should not be part of the business logic.
func (c *StashController) runBackupConfigurationProcessor(key string) error {
	obj, exists, err := c.bcInformer.GetIndexer().GetByKey(key)
	if err != nil {
		glog.Errorf("Fetching object with key %s from store failed with %v", key, err)
		return err
	}
	if !exists {
		glog.Warningf("BackupConfiguration %s does not exit anymore\n", key)
		return nil
	}

	backupConfiguration := obj.(*api_v1beta1.BackupConfiguration)
	glog.Infof("Sync/Add/Update for BackupConfiguration %s", backupConfiguration.GetName())
	// process syc/add/update event
	err = c.applyBackupConfigurationReconciliationLogic(backupConfiguration)
	if err != nil {
		return err
	}

	// We have successfully completed respective stuffs for the current state of this resource.
	// Hence, let's set observed generation as same as the current generation.
	_, err = v1beta1_util.UpdateBackupConfigurationStatus(c.stashClient.StashV1beta1(), backupConfiguration, func(in *api_v1beta1.BackupConfigurationStatus) *api_v1beta1.BackupConfigurationStatus {
		in.ObservedGeneration = backupConfiguration.Generation
		return in
	})

	return err
}

func (c *StashController) applyBackupConfigurationReconciliationLogic(backupConfiguration *api_v1beta1.BackupConfiguration) error {
	// check if BackupConfiguration is being deleted. if it is being deleted then delete respective resources.
	if backupConfiguration.DeletionTimestamp != nil {
		if core_util.HasFinalizer(backupConfiguration.ObjectMeta, api_v1beta1.StashKey) {
			if backupConfiguration.Spec.Target != nil {
				err := c.EnsureV1beta1SidecarDeleted(backupConfiguration.Spec.Target.Ref, backupConfiguration.Namespace)
				if err != nil {
					ref, rerr := reference.GetReference(stash_scheme.Scheme, backupConfiguration)
					if rerr != nil {
						return errors.NewAggregate([]error{err, rerr})
					}
					return c.handleWorkloadControllerTriggerFailure(ref, err)
				}
			}
			if err := c.EnsureCronJobDeletedForBackupConfiguration(backupConfiguration); err != nil {
				return err
			}
			// Remove finalizer
			_, _, err := v1beta1_util.PatchBackupConfiguration(c.stashClient.StashV1beta1(), backupConfiguration, func(in *api_v1beta1.BackupConfiguration) *api_v1beta1.BackupConfiguration {
				in.ObjectMeta = core_util.RemoveFinalizer(in.ObjectMeta, api_v1beta1.StashKey)
				return in

			})
			if err != nil {
				return err
			}
		}
	} else {
		// add a finalizer so that we can remove respective resources before this BackupConfiguration is deleted
		_, _, err := v1beta1_util.PatchBackupConfiguration(c.stashClient.StashV1beta1(), backupConfiguration, func(in *api_v1beta1.BackupConfiguration) *api_v1beta1.BackupConfiguration {
			in.ObjectMeta = core_util.AddFinalizer(in.ObjectMeta, api_v1beta1.StashKey)
			return in
		})
		if err != nil {
			return err
		}
		// skip if BackupConfiguration paused
		if backupConfiguration.Spec.Paused {
			log.Infof("Skipping processing BackupConfiguration %s/%s. Reason: Backup Configuration is paused.", backupConfiguration.Namespace, backupConfiguration.Name)
			return nil
		}
		if backupConfiguration.Spec.Target != nil &&
			backupConfiguration.Spec.Driver != api_v1beta1.VolumeSnapshotter &&
			util.BackupModel(backupConfiguration.Spec.Target.Ref.Kind) == util.ModelSidecar {
			if err := c.EnsureV1beta1Sidecar(backupConfiguration.Spec.Target.Ref, backupConfiguration.Namespace); err != nil {
				ref, rerr := reference.GetReference(stash_scheme.Scheme, backupConfiguration)
				if rerr != nil {
					return errors.NewAggregate([]error{err, rerr})
				}
				return c.handleWorkloadControllerTriggerFailure(ref, err)
			}
		}
		// create a CronJob that will create BackupSession on each schedule
		err = c.EnsureCronJobForBackupConfiguration(backupConfiguration)
		if err != nil {
			return c.handleCronJobCreationFailure(backupConfiguration, err)
		}
	}
	return nil
}

// EnsureV1beta1SidecarDeleted send an event to workload respective controller
// the workload controller will take care of removing respective sidecar
func (c *StashController) EnsureV1beta1SidecarDeleted(targetRef api_v1beta1.TargetRef, namespace string) error {
	return c.sendEventToWorkloadQueue(
		targetRef.Kind,
		namespace,
		targetRef.Name,
	)
}

// EnsureV1beta1Sidecar send an event to workload respective controller
// the workload controller will take care of injecting backup sidecar
func (c *StashController) EnsureV1beta1Sidecar(targetRef api_v1beta1.TargetRef, namespace string) error {
	return c.sendEventToWorkloadQueue(
		targetRef.Kind,
		namespace,
		targetRef.Name,
	)
}

func (c *StashController) sendEventToWorkloadQueue(kind, namespace, resourceName string) error {
	switch kind {
	case workload_api.KindDeployment:
		if resource, err := c.dpLister.Deployments(namespace).Get(resourceName); err == nil {
			key, err := cache.MetaNamespaceKeyFunc(resource)
			if err == nil {
				c.dpQueue.GetQueue().Add(key)
			}
			return err
		}
	case workload_api.KindDaemonSet:
		if resource, err := c.dsLister.DaemonSets(namespace).Get(resourceName); err == nil {
			key, err := cache.MetaNamespaceKeyFunc(resource)
			if err == nil {
				c.dsQueue.GetQueue().Add(key)
			}
			return err
		}
	case workload_api.KindStatefulSet:
		if resource, err := c.ssLister.StatefulSets(namespace).Get(resourceName); err == nil {
			key, err := cache.MetaNamespaceKeyFunc(resource)
			if err == nil {
				c.ssQueue.GetQueue().Add(key)
			}
		}
	case workload_api.KindReplicationController:
		if resource, err := c.rcLister.ReplicationControllers(namespace).Get(resourceName); err == nil {
			key, err := cache.MetaNamespaceKeyFunc(resource)
			if err == nil {
				c.rcQueue.GetQueue().Add(key)
			}
			return err
		}
	case workload_api.KindReplicaSet:
		if resource, err := c.rsLister.ReplicaSets(namespace).Get(resourceName); err == nil {
			key, err := cache.MetaNamespaceKeyFunc(resource)
			if err == nil {
				c.rsQueue.GetQueue().Add(key)
			}
			return err
		}
	case workload_api.KindDeploymentConfig:
		if c.ocClient != nil && c.dcLister != nil {
			if resource, err := c.dcLister.DeploymentConfigs(namespace).Get(resourceName); err == nil {
				key, err := cache.MetaNamespaceKeyFunc(resource)
				if err == nil {
					c.dcQueue.GetQueue().Add(key)
				}
				return err
			}
		}
	}
	return nil
}

// EnsureCronJob creates a Kubernetes CronJob for a BackupConfiguration object
func (c *StashController) EnsureCronJobForBackupConfiguration(backupConfig *api_v1beta1.BackupConfiguration) error {
	if backupConfig == nil {
		return fmt.Errorf("BackupBatch is nil")
	}
	ref, err := reference.GetReference(stash_scheme.Scheme, backupConfig)
	if err != nil {
		return err
	}

	return c.EnsureCronJob(ref, backupConfig.Spec.RuntimeSettings.Pod, backupConfig.OffshootLabels(), backupConfig.Spec.Schedule)
}

func (c *StashController) EnsureCronJobDeletedForBackupConfiguration(backupConfig *api_v1beta1.BackupConfiguration) error {
	ref, err := reference.GetReference(stash_scheme.Scheme, backupConfig)
	if err != nil {
		return err
	}
	return c.EnsureCronJobDeleted(backupConfig.ObjectMeta, ref)
}

// EnsureCronJob creates a Kubernetes CronJob for a BackupConfiguration/Batch object
// the CornJob will create a BackupSession object in each schedule
// respective BackupSession controller will watch this BackupSession object and take backup instantly
func (c *StashController) EnsureCronJob(ref *core.ObjectReference, podRuntimeSettings *v1.PodRuntimeSettings, labels map[string]string, schedule string) error {
	image := docker.Docker{
		Registry: c.DockerRegistry,
		Image:    docker.ImageStash,
		Tag:      c.StashImageTag,
	}

	meta := metav1.ObjectMeta{
		Name:      getBackupCronJobName(ref.Name),
		Namespace: ref.Namespace,
		Labels:    labels,
	}

	// ensure respective ClusterRole,RoleBinding,ServiceAccount etc.
	var serviceAccountName string

	if podRuntimeSettings != nil &&
		podRuntimeSettings.ServiceAccountName != "" {
		// ServiceAccount has been specified, so use it.
		serviceAccountName = podRuntimeSettings.ServiceAccountName
	} else {
		// ServiceAccount hasn't been specified. so create new one with same name as BackupConfiguration object.
		serviceAccountName = meta.Name

		_, _, err := core_util.CreateOrPatchServiceAccount(c.kubeClient, meta, func(in *core.ServiceAccount) *core.ServiceAccount {
			core_util.EnsureOwnerReference(&in.ObjectMeta, ref)
			return in
		})
		if err != nil {
			return err
		}
	}

	// now ensure RBAC stuff for this CronJob
	err := stash_rbac.EnsureCronJobRBAC(c.kubeClient, ref, serviceAccountName, c.getBackupSessionCronJobPSPNames(), labels)
	if err != nil {
		return err
	}

	_, _, err = batch_util.CreateOrPatchCronJob(c.kubeClient, meta, func(in *batch_v1beta1.CronJob) *batch_v1beta1.CronJob {
		//set backup-configuration as cron-job owner
		core_util.EnsureOwnerReference(&in.ObjectMeta, ref)

		in.Spec.Schedule = schedule
		in.Spec.JobTemplate.Labels = labels
		// ensure that job gets deleted on completion
		in.Spec.JobTemplate.Labels[apis.KeyDeleteJobOnCompletion] = apis.AllowDeletingJobOnCompletion

		in.Spec.JobTemplate.Spec.Template.Spec.Containers = core_util.UpsertContainer(
			in.Spec.JobTemplate.Spec.Template.Spec.Containers,
			core.Container{
				Name:            util.StashContainer,
				ImagePullPolicy: core.PullIfNotPresent,
				Image:           image.ToContainerImage(),
				Args: []string{
					"create-backupsession",
					fmt.Sprintf("--invokername=%s", ref.Name),
					fmt.Sprintf("--invokertype=%s", ref.Kind),
				},
			})
		in.Spec.JobTemplate.Spec.Template.Spec.RestartPolicy = core.RestartPolicyNever
		in.Spec.JobTemplate.Spec.Template.Spec.ServiceAccountName = serviceAccountName
		// insert default pod level security context
		in.Spec.JobTemplate.Spec.Template.Spec.SecurityContext = util.UpsertDefaultPodSecurityContext(in.Spec.JobTemplate.Spec.Template.Spec.SecurityContext)
		return in
	})

	return err
}

// EnsureCronJobDelete ensure that respective CronJob of a BackupConfiguration/BackupBatch has it as owner.
// Kuebernetes garbage collector will take care of removing the CronJob
func (c *StashController) EnsureCronJobDeleted(meta metav1.ObjectMeta, ref *core.ObjectReference) error {
	cur, err := c.kubeClient.BatchV1beta1().CronJobs(meta.Namespace).Get(getBackupCronJobName(meta.Name), metav1.GetOptions{})
	if err != nil {
		if kerr.IsNotFound(err) {
			return nil
		}
		return err
	}

	_, _, err = batch_util.PatchCronJob(c.kubeClient, cur, func(in *batch_v1beta1.CronJob) *batch_v1beta1.CronJob {
		core_util.EnsureOwnerReference(&in.ObjectMeta, ref)
		return in
	})
	return err
}

func getBackupCronJobName(name string) string {
	return strings.ReplaceAll(name, ".", "-")
}

func (c *StashController) handleCronJobCreationFailure(obj interface{}, err error) error {
	switch b := obj.(type) {
	case *api_v1beta1.BackupConfiguration:
		log.Warningf("failed to ensure cron job for BackupConfiguration %s/%s. Reason: %v", b.Namespace, b.Name, err)

		// write event to BackupConfiguration
		_, err2 := eventer.CreateEvent(
			c.kubeClient,
			eventer.EventSourceBackupConfigurationController,
			b,
			core.EventTypeWarning,
			eventer.EventReasonCronJobCreationFailed,
			fmt.Sprintf("failed to ensure CronJob for BackupConfiguration  %s/%s. Reason: %v", b.Namespace, b.Name, err),
		)
		return errors.NewAggregate([]error{err, err2})
	case *api_v1beta1.BackupBatch:
		log.Warningf("failed to ensure cron job for BackupBatch %s/%s. Reason: %v", b.Namespace, b.Name, err)

		// write event to BackupBatch
		_, err2 := eventer.CreateEvent(
			c.kubeClient,
			eventer.EventSourceBackupBatchController,
			b,
			core.EventTypeWarning,
			eventer.EventReasonCronJobCreationFailed,
			fmt.Sprintf("failed to ensure CronJob for BackupBatch  %s/%s. Reason: %v", b.Namespace, b.Name, err),
		)
		return errors.NewAggregate([]error{err, err2})

	}
	return err
}

func (c *StashController) handleWorkloadControllerTriggerFailure(ref *core.ObjectReference, err error) error {
	var eventSource string
	switch ref.Kind {
	case api_v1beta1.ResourceKindBackupConfiguration:
		eventSource = eventer.EventSourceBackupConfigurationController
	case api_v1beta1.ResourceKindBackupBatch:
		eventSource = eventer.EventSourceBackupBatchController
	case api_v1beta1.ResourceKindRestoreSession:
		eventSource = eventer.EventSourceRestoreSessionController
	}

	log.Warningf("failed to trigger workload controller for %s %s/%s. Reason: %v", ref.Kind, ref.Namespace, ref.Name, err)

	// write event to BackupConfiguration/RestoreSession
	_, err2 := eventer.CreateEvent(
		c.kubeClient,
		eventSource,
		ref,
		core.EventTypeWarning,
		eventer.EventReasonWorkloadControllerTriggeringFailed,
		fmt.Sprintf("failed to trigger workload controller for %s %s/%s. Reason: %v", ref.Kind, ref.Namespace, ref.Name, err),
	)
	return errors.NewAggregate([]error{err, err2})
}
