package controller

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/appscode/go/log"
	"github.com/golang/glog"
	batchv1 "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/reference"
	batch_util "kmodules.xyz/client-go/batch/v1"
	core_util "kmodules.xyz/client-go/core/v1"
	"kmodules.xyz/client-go/meta"
	"kmodules.xyz/client-go/tools/queue"
	"kmodules.xyz/webhook-runtime/admission"
	hooks "kmodules.xyz/webhook-runtime/admission/v1beta1"
	webhook "kmodules.xyz/webhook-runtime/admission/v1beta1/generic"
	"stash.appscode.dev/stash/apis"
	"stash.appscode.dev/stash/apis/stash"
	"stash.appscode.dev/stash/apis/stash/v1alpha1"
	api_v1beta1 "stash.appscode.dev/stash/apis/stash/v1beta1"
	stash_scheme "stash.appscode.dev/stash/client/clientset/versioned/scheme"
	stash_util "stash.appscode.dev/stash/client/clientset/versioned/typed/stash/v1beta1/util"
	"stash.appscode.dev/stash/pkg/docker"
	"stash.appscode.dev/stash/pkg/eventer"
	stash_rbac "stash.appscode.dev/stash/pkg/rbac"
	"stash.appscode.dev/stash/pkg/resolve"
	"stash.appscode.dev/stash/pkg/restic"
	"stash.appscode.dev/stash/pkg/util"
)

const (
	BackupJobPrefix                = "stash-backup-"
	VolumeSnapshotPrefix           = "volume-snapshot-"
	PromJobBackupSessionController = "stash-backupsession-controller"
)

func (c *StashController) NewBackupSessionWebhook() hooks.AdmissionHook {
	return webhook.NewGenericWebhook(
		schema.GroupVersionResource{
			Group:    "admission.stash.appscode.com",
			Version:  "v1beta1",
			Resource: "backupsessionvalidators",
		},
		"backupsessionvalidator",
		[]string{stash.GroupName},
		api.SchemeGroupVersion.WithKind(api_v1beta1.ResourceKindBackupSession),
		nil,
		&admission.ResourceHandlerFuncs{
			CreateFunc: func(obj runtime.Object) (runtime.Object, error) {
				return nil, obj.(*api_v1beta1.BackupSession).IsValid()
			},
			UpdateFunc: func(oldObj, newObj runtime.Object) (runtime.Object, error) {
				// should not allow spec update
				if !meta.Equal(oldObj.(*api_v1beta1.BackupSession).Spec, newObj.(*api_v1beta1.BackupSession).Spec) {
					return nil, fmt.Errorf("BackupSession spec is immutable")
				}
				return nil, nil
			},
		},
	)
}

// process only add events
func (c *StashController) initBackupSessionWatcher() {
	c.backupSessionInformer = c.stashInformerFactory.Stash().V1beta1().BackupSessions().Informer()
	c.backupSessionQueue = queue.New(api_v1beta1.ResourceKindBackupSession, c.MaxNumRequeues, c.NumThreads, c.runBackupSessionProcessor)
	c.backupSessionInformer.AddEventHandler(queue.NewObservableHandler(c.backupSessionQueue.GetQueue(), apis.EnableStatusSubresource))
	c.backupSessionLister = c.stashInformerFactory.Stash().V1beta1().BackupSessions().Lister()
}

func (c *StashController) runBackupSessionProcessor(key string) error {
	obj, exists, err := c.backupSessionInformer.GetIndexer().GetByKey(key)
	if err != nil {
		glog.Errorf("Fetching object with key %s from store failed with %v", key, err)
		return err
	}
	if !exists {
		glog.Warningf("BackupSession %s does not exist anymore\n", key)
		return nil
	}

	backupSession := obj.(*api_v1beta1.BackupSession)
	glog.Infof("Sync/Add/Update for BackupSession %s", backupSession.GetName())

	if backupSession.Status.Phase == api_v1beta1.BackupSessionFailed ||
		backupSession.Status.Phase == api_v1beta1.BackupSessionSucceeded {
		log.Infof("Skipping processing BackupSession %s/%s. Reason: phase is %q.", backupSession.Namespace, backupSession.Name, backupSession.Status.Phase)
		return nil
	}

	// check whether backup session is completed or running and set it's phase accordingly
	phase, err := c.getBackupSessionPhase(backupSession)

	if phase == api_v1beta1.BackupSessionFailed {
		// one or more hosts has failed to complete their backup process.
		// mark entire backup session as failure.
		// individual hosts has updated their respective stats and has sent respective metrics.
		// now, just set BackupSession phase "Failed" and create an event.
		return c.setBackupSessionFailed(backupSession, err)
	} else if phase == api_v1beta1.BackupSessionSucceeded {
		// all hosts has completed their backup process successfully.
		// individual hosts has updated their respective stats and has sent respective metrics.
		// now, just set BackupSession phase "Succeeded" and create an event.
		return c.setBackupSessionSucceeded(backupSession)
	} else if phase == api_v1beta1.BackupSessionRunning {
		log.Infof("Skipping processing BackupSession %s/%s. Reason: phase is %q.", backupSession.Namespace, backupSession.Name, backupSession.Status.Phase)
		return nil
	} else if phase == api_v1beta1.BackupSessionSkipped {
		log.Infof("Skipping processing BackupSession %s/%s. Reason: previously skipped.", backupSession.Namespace, backupSession.Name)
		return nil
	}

	// backup process for this BackupSession has not started. so let's start backup process
	// get BackupConfiguration for BackupSession
	backupConfig, err := c.stashClient.StashV1beta1().BackupConfigurations(backupSession.Namespace).Get(
		backupSession.Spec.BackupConfiguration.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		return fmt.Errorf("can't get BackupConfiguration for BackupSession %s/%s, Reason: %s", backupSession.Namespace, backupSession.Name, err)
	}

	// skip if BackupConfiguration paused
	if backupConfig.Spec.Paused {
		log.Infof("Skipping processing BackupSession %s/%s. Reason: Backup Configuration is paused.", backupSession.Namespace, backupSession.Name)
		return c.setBackupSessionSkipped(backupSession, fmt.Sprintf("BackupConfiguration %s/%s is paused", backupConfig.Namespace, backupConfig.Name))
	}

	// skip if backup model is sidecar.
	// for sidecar model controller inside sidecar will take care of it.
	if backupConfig.Spec.Target != nil && util.BackupModel(backupConfig.Spec.Target.Ref.Kind) == util.ModelSidecar {
		log.Infof("Skipping processing BackupSession %s/%s. Reason: Backup model is sidecar. Controller inside sidecar will take care of it.", backupSession.Namespace, backupSession.Name)
		return c.setBackupSessionRunning(backupSession)
	}

	// if VolumeSnapshotter driver is used then ensure VolumeSnapshotter job
	if backupConfig.Spec.Target != nil && backupConfig.Spec.Driver == api_v1beta1.VolumeSnapshotter {
		err := c.ensureVolumeSnapshotterJob(backupConfig, backupSession)
		if err != nil {
			return c.handleBackupJobCreationFailure(backupSession, err)
		}
		// VolumeSnapshotter job has been created successfully. Set BackupSession phase "Running"
		return c.setBackupSessionRunning(backupSession)
	}

	// Restic driver has been used. Now, create a backup job
	err = c.ensureBackupJob(backupSession, backupConfig)
	if err != nil {
		// failed to ensure backup job. set BackupSession phase "Failed" and send failure metrics.
		return c.handleBackupJobCreationFailure(backupSession, err)
	}

	// Backup job has been created successfully. Set BackupSession phase "Running"
	return c.setBackupSessionRunning(backupSession)
}

func (c *StashController) ensureBackupJob(backupSession *api_v1beta1.BackupSession, backupConfig *api_v1beta1.BackupConfiguration) error {
	offshootLabels := backupConfig.OffshootLabels()

	jobMeta := metav1.ObjectMeta{
		Name:      getBackupJobName(backupSession),
		Namespace: backupSession.Namespace,
		Labels:    offshootLabels,
	}

	backupConfigRef, err := reference.GetReference(stash_scheme.Scheme, backupConfig)
	if err != nil {
		return err
	}

	var serviceAccountName string

	// if RBAC is enabled then ensure respective RBAC stuffs
	if backupConfig.Spec.RuntimeSettings.Pod != nil && backupConfig.Spec.RuntimeSettings.Pod.ServiceAccountName != "" {
		serviceAccountName = backupConfig.Spec.RuntimeSettings.Pod.ServiceAccountName
	} else {
		// ServiceAccount hasn't been specified. so create new one.
		serviceAccountName = getBackupJobServiceAccountName(backupConfig)
		saMeta := metav1.ObjectMeta{
			Name:      serviceAccountName,
			Namespace: backupConfig.Namespace,
			Labels:    offshootLabels,
		}
		_, _, err = core_util.CreateOrPatchServiceAccount(c.kubeClient, saMeta, func(in *core.ServiceAccount) *core.ServiceAccount {
			core_util.EnsureOwnerReference(&in.ObjectMeta, backupConfigRef)
			return in
		})
	}

	psps, err := c.getBackupJobPSPNames(backupConfig)
	if err != nil {
		return err
	}

	err = stash_rbac.EnsureBackupJobRBAC(c.kubeClient, backupConfigRef, serviceAccountName, psps, offshootLabels)
	if err != nil {
		return err
	}

	// resolve task template

	explicitInputs := make(map[string]string)
	for _, param := range backupConfig.Spec.Task.Params {
		explicitInputs[param.Name] = param.Value
	}

	bcInputs, err := c.inputsForBackupConfig(*backupConfig)
	if err != nil {
		return fmt.Errorf("cannot resolve implicit inputs for BackupConfiguration %s/%s, reason: %s", backupConfig.Namespace, backupConfig.Name, err)
	}
	implicitInputs := bcInputs

	var repository *v1alpha1.Repository
	//  repository information is not present for VolumeSnapshotter driver
	if backupConfig.Spec.Driver != api_v1beta1.VolumeSnapshotter {
		repository, err = c.stashClient.StashV1alpha1().Repositories(backupConfig.Namespace).Get(
			backupConfig.Spec.Repository.Name,
			metav1.GetOptions{},
		)
		if err != nil {
			return err
		}
		repoInputs, err := c.inputsForRepository(repository)
		if err != nil {
			return fmt.Errorf("cannot resolve implicit inputs for Repository %s/%s, reason: %s", repository.Namespace, repository.Name, err)
		}
		implicitInputs = core_util.UpsertMap(implicitInputs, repoInputs)
	}

	implicitInputs[apis.Namespace] = backupSession.Namespace
	implicitInputs[apis.BackupSession] = backupSession.Name
	implicitInputs[apis.StatusSubresourceEnabled] = fmt.Sprint(apis.EnableStatusSubresource)

	taskResolver := resolve.TaskResolver{
		StashClient:     c.stashClient,
		TaskName:        backupConfig.Spec.Task.Name,
		Inputs:          core_util.UpsertMap(explicitInputs, implicitInputs), // TODO: reverse priority ???
		RuntimeSettings: backupConfig.Spec.RuntimeSettings,
		TempDir:         backupConfig.Spec.TempDir,
	}
	podSpec, err := taskResolver.GetPodSpec()
	if err != nil {
		return fmt.Errorf("can't get PodSpec for BackupConfiguration %s/%s, reason: %s", backupConfig.Namespace, backupConfig.Name, err)
	}
	// for local backend, attach volume to all containers
	if repository != nil && repository.Spec.Backend.Local != nil {
		podSpec = util.AttachLocalBackend(podSpec, *repository.Spec.Backend.Local)
	}

	// upsert InterimVolume to hold the backup/restored data temporarily
	backupSessionRef, err := reference.GetReference(stash_scheme.Scheme, backupSession)
	if err != nil {
		return err
	}
	podSpec, err = util.UpsertInterimVolume(c.kubeClient, podSpec, backupConfig.Spec.InterimVolumeTemplate, backupSessionRef)
	if err != nil {
		return err
	}

	// create Backup Job
	_, _, err = batch_util.CreateOrPatchJob(c.kubeClient, jobMeta, func(in *batchv1.Job) *batchv1.Job {
		// set BackupSession as owner of this Job
		core_util.EnsureOwnerReference(&in.ObjectMeta, backupSessionRef)

		in.Spec.Template.Spec = podSpec
		in.Spec.Template.Spec.ServiceAccountName = serviceAccountName
		return in
	})

	return err
}

func (c *StashController) ensureVolumeSnapshotterJob(backupConfig *api_v1beta1.BackupConfiguration, backupSession *api_v1beta1.BackupSession) error {
	offshootLabels := backupConfig.OffshootLabels()

	jobMeta := metav1.ObjectMeta{
		Name:      getVolumeSnapshotterJobName(backupSession),
		Namespace: backupSession.Namespace,
		Labels:    offshootLabels,
	}

	backupConfigRef, err := reference.GetReference(stash_scheme.Scheme, backupConfig)
	if err != nil {
		return err
	}

	//ensure respective RBAC stuffs
	//Create new ServiceAccount
	serviceAccountName := backupConfig.Name
	saMeta := metav1.ObjectMeta{
		Name:      serviceAccountName,
		Namespace: backupConfig.Namespace,
		Labels:    offshootLabels,
	}
	_, _, err = core_util.CreateOrPatchServiceAccount(c.kubeClient, saMeta, func(in *core.ServiceAccount) *core.ServiceAccount {
		core_util.EnsureOwnerReference(&in.ObjectMeta, backupConfigRef)
		return in
	})
	if err != nil {
		return err
	}

	err = stash_rbac.EnsureVolumeSnapshotterJobRBAC(c.kubeClient, backupConfigRef, serviceAccountName, offshootLabels)
	if err != nil {
		return err
	}

	image := docker.Docker{
		Registry: c.DockerRegistry,
		Image:    docker.ImageStash,
		Tag:      c.StashImageTag,
	}

	jobTemplate, err := util.NewVolumeSnapshotterJob(backupSession, backupConfig, image)
	if err != nil {
		return err
	}

	// Create VolumeSnapshotter job
	_, _, err = batch_util.CreateOrPatchJob(c.kubeClient, jobMeta, func(in *batchv1.Job) *batchv1.Job {
		// set BackupSession as owner of this Job
		core_util.EnsureOwnerReference(&in.ObjectMeta, backupConfigRef)

		in.Labels = offshootLabels
		in.Spec.Template = *jobTemplate
		in.Spec.Template.Spec.ServiceAccountName = serviceAccountName
		return in
	})

	return err
}

func (c *StashController) setBackupSessionFailed(backupSession *api_v1beta1.BackupSession, backupErr error) error {

	// set BackupSession phase to "Failed"
	updatedBackupSession, err := stash_util.UpdateBackupSessionStatus(c.stashClient.StashV1beta1(), backupSession, func(in *api_v1beta1.BackupSessionStatus) *api_v1beta1.BackupSessionStatus {
		in.Phase = api_v1beta1.BackupSessionFailed
		return in
	}, apis.EnableStatusSubresource)
	if err != nil {
		return err
	}

	// write failure event to BackupSession
	_, _ = eventer.CreateEvent(
		c.kubeClient,
		eventer.EventSourceBackupSessionController,
		backupSession,
		core.EventTypeWarning,
		eventer.EventReasonBackupSessionFailed,
		fmt.Sprintf("Backup session failed to complete. Reason: %v", backupErr),
	)

	// send backup session specific metrics
	backupConfig, err2 := c.stashClient.StashV1beta1().BackupConfigurations(backupSession.Namespace).Get(backupSession.Spec.BackupConfiguration.Name, metav1.GetOptions{})
	if err2 != nil {
		return err2
	}
	metricsOpt := &restic.MetricsOptions{
		Enabled:        true,
		PushgatewayURL: util.PushgatewayLocalURL,
		JobName:        PromJobBackupSessionController,
	}
	err = metricsOpt.SendBackupSessionMetrics(c.clientConfig, backupConfig, updatedBackupSession.Status)
	if err != nil {
		return err
	}

	// cleanup old BackupSessions
	return c.cleanupBackupHistory(backupConfig)
}

func (c *StashController) setBackupSessionSkipped(backupSession *api_v1beta1.BackupSession, reason string) error {
	// set BackupSession phase to "Skipped"
	_, err := stash_util.UpdateBackupSessionStatus(c.stashClient.StashV1beta1(), backupSession, func(in *api_v1beta1.BackupSessionStatus) *api_v1beta1.BackupSessionStatus {
		in.Phase = api_v1beta1.BackupSessionSkipped
		return in
	}, apis.EnableStatusSubresource)
	if err != nil {
		return err
	}

	// write skip event
	_, err = eventer.CreateEvent(
		c.kubeClient,
		eventer.EventSourceBackupSessionController,
		backupSession,
		core.EventTypeWarning,
		eventer.EventReasonBackupSessionSkipped,
		reason,
	)
	return err
}

func (c *StashController) setBackupSessionRunning(backupSession *api_v1beta1.BackupSession) error {

	backupConfig, err := c.stashClient.StashV1beta1().BackupConfigurations(backupSession.Namespace).Get(
		backupSession.Spec.BackupConfiguration.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		return err
	}

	// find out the total number of hosts that will be backed up in this backup session
	totalHosts, err := c.getTotalHosts(backupConfig.Spec.Target, backupConfig.Namespace, backupConfig.Spec.Driver)
	if err != nil {
		return err
	}

	// set BackupSession phase to "Running"
	_, err = stash_util.UpdateBackupSessionStatus(c.stashClient.StashV1beta1(), backupSession, func(in *api_v1beta1.BackupSessionStatus) *api_v1beta1.BackupSessionStatus {
		in.Phase = api_v1beta1.BackupSessionRunning
		in.TotalHosts = totalHosts
		return in
	}, apis.EnableStatusSubresource)
	if err != nil {
		return err
	}

	// write event to the BackupSession
	_, err = eventer.CreateEvent(
		c.kubeClient,
		eventer.EventSourceBackupSessionController,
		backupSession,
		core.EventTypeNormal,
		eventer.EventReasonBackupSessionRunning,
		fmt.Sprintf("Backup job has been created succesfully/sidecar is watching the BackupSession."),
	)
	return err
}

func (c *StashController) setBackupSessionSucceeded(backupSession *api_v1beta1.BackupSession) error {

	// total backup session duration is the difference between the time when BackupSession was created and current time
	sessionDuration := time.Since(backupSession.CreationTimestamp.Time)

	// set BackupSession phase "Succeeded"
	updatedBackupSession, err := stash_util.UpdateBackupSessionStatus(c.stashClient.StashV1beta1(), backupSession, func(in *api_v1beta1.BackupSessionStatus) *api_v1beta1.BackupSessionStatus {
		in.Phase = api_v1beta1.BackupSessionSucceeded
		in.SessionDuration = sessionDuration.String()
		return in
	}, apis.EnableStatusSubresource)
	if err != nil {
		return err
	}

	// write event to the BackupSession for successful backup
	_, err = eventer.CreateEvent(
		c.kubeClient,
		eventer.EventSourceBackupSessionController,
		backupSession,
		core.EventTypeNormal,
		eventer.EventReasonBackupSessionSucceeded,
		fmt.Sprintf("Backup session completed successfully"),
	)

	// send backup session specific metrics
	backupConfig, err := c.stashClient.StashV1beta1().BackupConfigurations(backupSession.Namespace).Get(backupSession.Spec.BackupConfiguration.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	metricsOpt := &restic.MetricsOptions{
		Enabled:        true,
		PushgatewayURL: util.PushgatewayLocalURL,
		JobName:        PromJobBackupSessionController,
	}
	err = metricsOpt.SendBackupSessionMetrics(c.clientConfig, backupConfig, updatedBackupSession.Status)
	if err != nil {
		return err
	}

	// cleanup old BackupSessions
	return c.cleanupBackupHistory(backupConfig)
}

func (c *StashController) getBackupSessionPhase(backupSession *api_v1beta1.BackupSession) (api_v1beta1.BackupSessionPhase, error) {
	// BackupSession phase is empty or "Pending" then return it. controller will process accordingly
	if backupSession.Status.TotalHosts == nil ||
		backupSession.Status.Phase == "" ||
		backupSession.Status.Phase == api_v1beta1.BackupSessionPending {
		return api_v1beta1.BackupSessionPending, nil
	}

	// all hosts hasn't completed it's backup. BackupSession phase must be "Running".
	if *backupSession.Status.TotalHosts != int32(len(backupSession.Status.Stats)) {
		return api_v1beta1.BackupSessionRunning, nil
	}

	// check if any of the host has failed to take backup. if any of them has failed, then consider entire backup session as a failure.
	for _, host := range backupSession.Status.Stats {
		if host.Phase == api_v1beta1.HostBackupFailed {
			return api_v1beta1.BackupSessionFailed, fmt.Errorf("backup failed for host: %s. Reason: %s", host.Hostname, host.Error)
		}
	}

	// backup has been completed successfully
	return api_v1beta1.BackupSessionSucceeded, nil
}

func (c *StashController) handleBackupJobCreationFailure(backupSession *api_v1beta1.BackupSession, err error) error {
	log.Warningln("failed to ensure backup job. Reason: ", err)

	// write event to BackupSession
	_, _ = eventer.CreateEvent(
		c.kubeClient,
		eventer.EventSourceBackupSessionController,
		backupSession,
		core.EventTypeWarning,
		eventer.EventReasonBackupJobCreationFailed,
		fmt.Sprintf("failed to create backup job for BackupSession %s/%s. Reason: %v", backupSession.Namespace, backupSession.Name, err),
	)

	// set BackupSession phase failed
	return c.setBackupSessionFailed(backupSession, err)
}

func getBackupJobName(backupSession *api_v1beta1.BackupSession) string {
	return BackupJobPrefix + strings.ReplaceAll(backupSession.Name, ".", "-")
}

func getBackupJobServiceAccountName(backupConfiguration *api_v1beta1.BackupConfiguration) string {
	return strings.ReplaceAll(backupConfiguration.Name, ".", "-")
}

func getVolumeSnapshotterJobName(backupSession *api_v1beta1.BackupSession) string {
	return VolumeSnapshotPrefix + strings.ReplaceAll(backupSession.Name, ".", "-")
}

// cleanupBackupHistory deletes old BackupSessions and theirs associate resources according to BackupHistoryLimit
func (c *StashController) cleanupBackupHistory(backupConfig *api_v1beta1.BackupConfiguration) error {
	// default history limit is 1
	historyLimit := int32(1)
	if backupConfig.Spec.BackupHistoryLimit != nil {
		historyLimit = *backupConfig.Spec.BackupHistoryLimit
	}

	// BackupSession use BackupConfiguration name as label. We can use this label as selector to list only the BackupSession
	// of this particular BackupConfiguration.
	label := metav1.LabelSelector{
		MatchLabels: map[string]string{
			util.LabelBackupConfiguration: backupConfig.Name,
		},
	}
	selector, err := metav1.LabelSelectorAsSelector(&label)
	if err != nil {
		return err
	}

	// list all the BackupSessions of this particular BackupConfiguration
	bsList, err := c.backupSessionLister.BackupSessions(backupConfig.Namespace).List(selector)
	if err != nil {
		return err
	}

	// sort BackupSession according to creation timestamp. keep latest BackupSession first.
	sort.Slice(bsList, func(i, j int) bool {
		return bsList[i].CreationTimestamp.After(bsList[j].CreationTimestamp.Time)
	})

	// delete the BackupSession that does not fit within the history limit
	for i := int(historyLimit); i < len(bsList); i++ {
		err = c.stashClient.StashV1beta1().BackupSessions(backupConfig.Namespace).Delete(bsList[i].Name, meta.DeleteInBackground())
		if err != nil && !(kerr.IsNotFound(err) || kerr.IsGone(err)) {
			return err
		}
	}
	return nil
}
