package framework

import (
	"stash.appscode.dev/stash/apis/stash/v1alpha1"
	"stash.appscode.dev/stash/apis/stash/v1beta1"

	"github.com/appscode/go/crypto/rand"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (f *Invocation) BackupBlueprint(repoInfo v1alpha1.RepositorySpec) *v1beta1.BackupBlueprint {

	return &v1beta1.BackupBlueprint{
		ObjectMeta: metav1.ObjectMeta{
			Name: rand.WithUniqSuffix(f.app),
		},
		Spec: v1beta1.BackupBlueprintSpec{
			RepositorySpec: repoInfo,
			Schedule:       "*/59 * * * *",
			RetentionPolicy: v1alpha1.RetentionPolicy{
				Name:     "keep-last-5",
				KeepLast: 5,
				Prune:    true,
			},
		},
	}
}

func (f *Framework) CreateBackupBlueprint(backupBlueprint *v1beta1.BackupBlueprint) (*v1beta1.BackupBlueprint, error) {
	return f.StashClient.StashV1beta1().BackupBlueprints().Create(backupBlueprint)
}

func (f *Invocation) DeleteBackupBlueprint(name string) error {
	if name == "" {
		return nil
	}
	err := f.StashClient.StashV1beta1().BackupBlueprints().Delete(name, &metav1.DeleteOptions{})
	if kerr.IsNotFound(err) {
		return nil
	}
	return err
}

func (f *Framework) GetBackupBlueprint(name string) (*v1beta1.BackupBlueprint, error) {
	return f.StashClient.StashV1beta1().BackupBlueprints().Get(name, metav1.GetOptions{})
}
