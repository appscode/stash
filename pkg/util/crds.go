package util

import (
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api_v1beta1 "stash.appscode.dev/stash/apis/stash/v1beta1"
	cs "stash.appscode.dev/stash/client/clientset/versioned"
	"stash.appscode.dev/stash/pkg/docker"
)

func EnsureDefaultFunctions(stashClient cs.Interface, registry, imageTag string) error {
	image := docker.Docker{
		Registry: registry,
		Image:    docker.ImageStash,
		Tag:      imageTag,
	}

	defaultFunctions := []*api_v1beta1.Function{
		updateStatusFunction(image),
		pvcBackupFunction(image),
		pvcRestoreFunction(image),
	}

	for _, fn := range defaultFunctions {
		_, err := stashClient.StashV1beta1().Functions().Create(fn)
		if err != nil && !kerr.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func EnsureDefaultTasks(stashClient cs.Interface) error {
	defaultTasks := []*api_v1beta1.Task{
		pvcBackupTask(),
		pvcRestoreTask(),
	}

	for _, task := range defaultTasks {
		_, err := stashClient.StashV1beta1().Tasks().Create(task)
		if err != nil && !kerr.IsAlreadyExists(err) {
			return err
		}
	}

	return nil
}

func updateStatusFunction(image docker.Docker) *api_v1beta1.Function {
	return &api_v1beta1.Function{
		ObjectMeta: metav1.ObjectMeta{
			Name: "update-status",
		},
		Spec: api_v1beta1.FunctionSpec{
			Image: image.ToContainerImage(),
			Args: []string{
				"update-status",
				"--namespace=${NAMESPACE:=default}",
				"--backup-session=${BACKUP_SESSION:=}",
				"--repository=${REPOSITORY_NAME:=}",
				"--restore-session=${RESTORE_SESSION:=}",
				"--output-dir=${outputDir:=}",
				"--enable-status-subresource=${ENABLE_STATUS_SUBRESOURCE:=false}",
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
				"--rest-server-url=${REPOSITORY_URL:=}",
				"--path=${REPOSITORY_PREFIX:=}",
				"--secret-dir=/etc/repository/secret",
				"--scratch-dir=/tmp",
				"--enable-cache=${ENABLE_CACHE:=true}",
				"--max-connections=${MAX_CONNECTIONS:=0}",
				"--hostname=${HOSTNAME:=}",
				"--backup-dirs=${TARGET_DIRECTORIES}",
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
				"--metrics-enabled=true",
				"--metrics-pushgateway-url=${PROMETHEUS_PUSHGATEWAY_URL:=}",
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
				"--rest-server-url=${REPOSITORY_URL:=}",
				"--path=${REPOSITORY_PREFIX:=}",
				"--secret-dir=/etc/repository/secret",
				"--scratch-dir=/tmp",
				"--enable-cache=${ENABLE_CACHE:=true}",
				"--max-connections=${MAX_CONNECTIONS:=0}",
				"--hostname=${HOSTNAME:=}",
				"--restore-dirs=${RESTORE_DIRECTORIES}",
				"--snapshots=${RESTORE_SNAPSHOTS:=}",
				"--output-dir=${outputDir:=}",
				"--metrics-enabled=true",
				"--metrics-pushgateway-url=${PROMETHEUS_PUSHGATEWAY_URL:=}",
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
							Value: "target-volume",
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
					},
				},
			},
			Volumes: []core.Volume{
				{
					Name: "target-volume",
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
							Value: "target-volume",
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
					},
				},
			},
			Volumes: []core.Volume{
				{
					Name: "target-volume",
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
