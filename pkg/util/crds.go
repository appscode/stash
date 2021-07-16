/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"context"
	"fmt"

	"stash.appscode.dev/apimachinery/apis"
	api_v1beta1 "stash.appscode.dev/apimachinery/apis/stash/v1beta1"
	cs "stash.appscode.dev/apimachinery/client/clientset/versioned"
	util_v1beta1 "stash.appscode.dev/apimachinery/client/clientset/versioned/typed/stash/v1beta1/util"
	"stash.appscode.dev/apimachinery/pkg/docker"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"kmodules.xyz/client-go/tools/pushgateway"
)

// EnsureDefaultFunctions creates "update-status", "pvc-backup" and "pvc-restore" Functions if they are not already present
func EnsureDefaultFunctions(stashClient cs.Interface, registry, stashImage, imageTag string) error {
	image := docker.Docker{
		Registry: registry,
		Image:    stashImage,
		Tag:      imageTag,
	}

	defaultFunctions := []*api_v1beta1.Function{
		updateStatusFunction(image),
		pvcBackupFunction(image),
		pvcRestoreFunction(image),
	}

	for _, fn := range defaultFunctions {
		_, _, err := util_v1beta1.CreateOrPatchFunction(
			context.TODO(),
			stashClient.StashV1beta1(),
			fn.ObjectMeta,
			func(in *api_v1beta1.Function) *api_v1beta1.Function {
				in.Spec = fn.Spec
				return in
			},
			metav1.PatchOptions{},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// EnsureDefaultTasks creates "pvc-backup" and "pvc-restore" Tasks if they are not already present
func EnsureDefaultTasks(stashClient cs.Interface) error {
	defaultTasks := []*api_v1beta1.Task{
		pvcBackupTask(),
		pvcRestoreTask(),
	}

	for _, task := range defaultTasks {
		_, _, err := util_v1beta1.CreateOrPatchTask(
			context.TODO(),
			stashClient.StashV1beta1(),
			task.ObjectMeta,
			func(in *api_v1beta1.Task) *api_v1beta1.Task {
				in.Spec = task.Spec
				return in
			},
			metav1.PatchOptions{},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func updateStatusFunction(image docker.Docker) *api_v1beta1.Function {
	return &api_v1beta1.Function{
		TypeMeta: metav1.TypeMeta{
			APIVersion: api_v1beta1.SchemeGroupVersion.String(),
			Kind:       api_v1beta1.ResourceKindFunction,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "update-status",
		},
		Spec: api_v1beta1.FunctionSpec{
			Image: image.ToContainerImage(),
			Args: []string{
				"update-status",
				"--provider=${REPOSITORY_PROVIDER:=}",
				"--bucket=${REPOSITORY_BUCKET:=}",
				"--endpoint=${REPOSITORY_ENDPOINT:=}",
				"--region=${REPOSITORY_REGION:=}",
				"--path=${REPOSITORY_PREFIX:=}",
				"--secret-dir=/etc/repository/secret",
				"--scratch-dir=/tmp",
				"--enable-cache=${ENABLE_CACHE:=true}",
				"--max-connections=${MAX_CONNECTIONS:=0}",
				"--namespace=${NAMESPACE:=default}",
				"--backupsession=${BACKUP_SESSION:=}",
				"--repository=${REPOSITORY_NAME:=}",
				"--invoker-kind=${INVOKER_KIND:=}",
				"--invoker-name=${INVOKER_NAME:=}",
				"--target-kind=${TARGET_KIND:=}",
				"--target-name=${TARGET_NAME:=}",
				"--output-dir=${outputDir:=}",
				"--metrics-enabled=true",
				fmt.Sprintf("--metrics-pushgateway-url=%s", pushgateway.URL()),
			},
			VolumeMounts: []core.VolumeMount{
				{
					Name:      "${secretVolume}",
					MountPath: "/etc/repository/secret",
				},
			},
		},
	}
}

func pvcBackupFunction(image docker.Docker) *api_v1beta1.Function {
	return &api_v1beta1.Function{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-backup",
		},
		Spec: api_v1beta1.FunctionSpec{
			Image: image.ToContainerImage(),
			Args: []string{
				"backup-pvc",
				"--provider=${REPOSITORY_PROVIDER:=}",
				"--bucket=${REPOSITORY_BUCKET:=}",
				"--endpoint=${REPOSITORY_ENDPOINT:=}",
				"--region=${REPOSITORY_REGION:=}",
				"--path=${REPOSITORY_PREFIX:=}",
				"--secret-dir=/etc/repository/secret",
				"--scratch-dir=/tmp",
				"--enable-cache=${ENABLE_CACHE:=true}",
				"--max-connections=${MAX_CONNECTIONS:=0}",
				"--hostname=${HOSTNAME:=}",
				"--backup-paths=${TARGET_PATHS}",
				"--exclude=${EXCLUDE_PATTERNS:=}",
				"--invoker-kind=${INVOKER_KIND:=}",
				"--invoker-name=${INVOKER_NAME:=}",
				"--target-kind=${TARGET_KIND:=}",
				"--target-name=${TARGET_NAME:=}",
				"--backupsession=${BACKUP_SESSION:=}",
				"--retention-keep-last=${RETENTION_KEEP_LAST:=0}",
				"--retention-keep-hourly=${RETENTION_KEEP_HOURLY:=0}",
				"--retention-keep-daily=${RETENTION_KEEP_DAILY:=0}",
				"--retention-keep-weekly=${RETENTION_KEEP_WEEKLY:=0}",
				"--retention-keep-monthly=${RETENTION_KEEP_MONTHLY:=0}",
				"--retention-keep-yearly=${RETENTION_KEEP_YEARLY:=0}",
				"--retention-keep-tags=${RETENTION_KEEP_TAGS:=}",
				"--retention-prune=${RETENTION_PRUNE:=false}",
				"--retention-dry-run=${RETENTION_DRY_RUN:=false}",
				"--output-dir=${outputDir:=}",
			},
			VolumeMounts: []core.VolumeMount{
				{
					Name:      "${targetVolume}",
					MountPath: "${TARGET_MOUNT_PATH}",
				},
				{
					Name:      "${secretVolume}",
					MountPath: "/etc/repository/secret",
				},
			},
		},
	}
}

func pvcRestoreFunction(image docker.Docker) *api_v1beta1.Function {
	return &api_v1beta1.Function{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-restore",
		},
		Spec: api_v1beta1.FunctionSpec{
			Image: image.ToContainerImage(),
			Args: []string{
				"restore-pvc",
				"--provider=${REPOSITORY_PROVIDER:=}",
				"--bucket=${REPOSITORY_BUCKET:=}",
				"--endpoint=${REPOSITORY_ENDPOINT:=}",
				"--region=${REPOSITORY_REGION:=}",
				"--path=${REPOSITORY_PREFIX:=}",
				"--secret-dir=/etc/repository/secret",
				"--scratch-dir=/tmp",
				"--enable-cache=${ENABLE_CACHE:=true}",
				"--max-connections=${MAX_CONNECTIONS:=0}",
				"--hostname=${HOSTNAME:=}",
				"--restore-paths=${RESTORE_PATHS}",
				"--include=${INCLUDE_PATTERNS:=}",
				"--exclude=${EXCLUDE_PATTERNS:=}",
				"--snapshots=${RESTORE_SNAPSHOTS:=}",
				"--output-dir=${outputDir:=}",
				"--invoker-kind=${INVOKER_KIND:=}",
				"--invoker-name=${INVOKER_NAME:=}",
				"--target-kind=${TARGET_KIND:=}",
				"--target-name=${TARGET_NAME:=}",
			},
			VolumeMounts: []core.VolumeMount{
				{
					Name:      "${targetVolume}",
					MountPath: "${TARGET_MOUNT_PATH}",
				},
				{
					Name:      "${secretVolume}",
					MountPath: "/etc/repository/secret",
				},
			},
		},
	}
}

func pvcBackupTask() *api_v1beta1.Task {
	return &api_v1beta1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-backup",
		},
		Spec: api_v1beta1.TaskSpec{
			Steps: []api_v1beta1.FunctionRef{
				{
					Name: "pvc-backup",
					Params: []api_v1beta1.Param{
						{
							Name:  "outputDir",
							Value: "/tmp/output",
						},
						{
							Name:  "targetVolume",
							Value: apis.StashDefaultVolume,
						},
						{
							Name:  "secretVolume",
							Value: "secret-volume",
						},
					},
				},
				{
					Name: "update-status",
					Params: []api_v1beta1.Param{
						{
							Name:  "outputDir",
							Value: "/tmp/output",
						},
						{
							Name:  "secretVolume",
							Value: "secret-volume",
						},
					},
				},
			},
			Volumes: []core.Volume{
				{
					Name: apis.StashDefaultVolume,
					VolumeSource: core.VolumeSource{
						PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
							ClaimName: "${TARGET_NAME}",
						},
					},
				},
				{
					Name: "secret-volume",
					VolumeSource: core.VolumeSource{
						Secret: &core.SecretVolumeSource{
							SecretName: "${REPOSITORY_SECRET_NAME}",
						},
					},
				},
			},
		},
	}
}

func pvcRestoreTask() *api_v1beta1.Task {
	return &api_v1beta1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-restore",
		},
		Spec: api_v1beta1.TaskSpec{
			Steps: []api_v1beta1.FunctionRef{
				{
					Name: "pvc-restore",
					Params: []api_v1beta1.Param{
						{
							Name:  "outputDir",
							Value: "/tmp/output",
						},
						{
							Name:  "targetVolume",
							Value: apis.StashDefaultVolume,
						},
						{
							Name:  "secretVolume",
							Value: "secret-volume",
						},
					},
				},
				{
					Name: "update-status",
					Params: []api_v1beta1.Param{
						{
							Name:  "outputDir",
							Value: "/tmp/output",
						},
						{
							Name:  "secretVolume",
							Value: "secret-volume",
						},
					},
				},
			},
			Volumes: []core.Volume{
				{
					Name: apis.StashDefaultVolume,
					VolumeSource: core.VolumeSource{
						PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
							ClaimName: "${TARGET_NAME}",
						},
					},
				},
				{
					Name: "secret-volume",
					VolumeSource: core.VolumeSource{
						Secret: &core.SecretVolumeSource{
							SecretName: "${REPOSITORY_SECRET_NAME}",
						},
					},
				},
			},
		},
	}
}
