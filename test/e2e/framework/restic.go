package framework

import (
	"github.com/appscode/go/crypto/rand"
	api "github.com/appscode/stash/apis/stash/v1alpha1"
	stash_util "github.com/appscode/stash/client/typed/stash/v1alpha1/util"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiv1 "k8s.io/client-go/pkg/api/v1"
)

func (fi *Invocation) _restic() api.Restic {
	return api.Restic{
		TypeMeta: metav1.TypeMeta{
			APIVersion: api.SchemeGroupVersion.String(),
			Kind:       api.ResourceKindRestic,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix("stash"),
			Namespace: fi.namespace,
		},
		Spec: api.ResticSpec{
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": fi.app,
				},
			},
			FileGroups: []api.FileGroup{
				{
					Path: TestSourceDataMountPath,
					RetentionPolicy: api.RetentionPolicy{
						KeepLast: 5,
					},
				},
			},
			Schedule: "@every 15s",
			VolumeMounts: []apiv1.VolumeMount{
				{
					Name:      TestSourceDataVolumeName,
					MountPath: TestSourceDataMountPath,
				},
			},
		},
	}
}

func (fi *Invocation) ResticForLocalBackend() api.Restic {
	r := fi._restic()
	r.Spec.Backend = api.Backend{
		StorageSecretName: "",
		Local: &api.LocalSpec{
			Path: "/safe/data",
			VolumeSource: apiv1.VolumeSource{
				EmptyDir: &apiv1.EmptyDirVolumeSource{},
			},
		},
	}
	return r
}

func (fi *Invocation) ResticForS3Backend() api.Restic {
	r := fi._restic()
	r.Spec.Backend = api.Backend{
		StorageSecretName: "",
		S3: &api.S3Spec{
			Endpoint: "s3.amazonaws.com",
			Bucket:   "stash-qa",
			Prefix:   fi.app,
		},
	}
	return r
}

func (fi *Invocation) ResticForGCSBackend() api.Restic {
	r := fi._restic()
	r.Spec.Backend = api.Backend{
		StorageSecretName: "",
		GCS: &api.GCSSpec{
			Bucket: "stash-qa",
			Prefix: fi.app,
		},
	}
	return r
}

func (fi *Invocation) ResticForAzureBackend() api.Restic {
	r := fi._restic()
	r.Spec.Backend = api.Backend{
		StorageSecretName: "",
		Azure: &api.AzureSpec{
			Container: "stashqa",
			Prefix:    fi.app,
		},
	}
	return r
}

func (fi *Invocation) ResticForSwiftBackend() api.Restic {
	r := fi._restic()
	r.Spec.Backend = api.Backend{
		StorageSecretName: "",
		Swift: &api.SwiftSpec{
			Container: "stash-qa",
			Prefix:    fi.app,
		},
	}
	return r
}

func (f *Framework) CreateRestic(obj api.Restic) error {
	_, err := f.StashClient.Restics(obj.Namespace).Create(&obj)
	return err
}

func (f *Framework) DeleteRestic(meta metav1.ObjectMeta) error {
	return f.StashClient.Restics(meta.Namespace).Delete(meta.Name, deleteInForeground())
}

func (f *Framework) UpdateRestic(meta metav1.ObjectMeta, transformer func(*api.Restic) *api.Restic) error {
	_, err := stash_util.TryUpdateRestic(f.StashClient, meta, transformer)
	return err
}

func (f *Framework) EventuallyRestic(meta metav1.ObjectMeta) GomegaAsyncAssertion {
	return Eventually(func() *api.Restic {
		obj, err := f.StashClient.Restics(meta.Namespace).Get(meta.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		return obj
	})
}
