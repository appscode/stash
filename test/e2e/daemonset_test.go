package e2e_test

import (
	"time"

	ext_util "github.com/appscode/kutil/extensions/v1beta1"
	api "github.com/appscode/stash/apis/stash/v1alpha1"
	"github.com/appscode/stash/pkg/util"
	"github.com/appscode/stash/test/e2e/framework"
	. "github.com/appscode/stash/test/e2e/matcher"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	core "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("DaemonSet", func() {
	var (
		err      error
		f        *framework.Invocation
		backup   api.Backup
		cred     core.Secret
		daemon   extensions.DaemonSet
		recovery api.Recovery
	)

	BeforeEach(func() {
		f = root.Invoke()
	})
	AfterEach(func() {
		time.Sleep(60 * time.Second)
	})
	JustBeforeEach(func() {
		if missing, _ := BeZero().Match(cred); missing {
			Skip("Missing repository credential")
		}
		backup.Spec.Backend.StorageSecretName = cred.Name
		recovery.Spec.Backend.StorageSecretName = cred.Name
		daemon = f.DaemonSet()
	})

	var (
		shouldBackupNewDaemonSet = func() {
			By("Creating repository Secret " + cred.Name)
			err = f.CreateSecret(cred)
			Expect(err).NotTo(HaveOccurred())

			By("Creating backup " + backup.Name)
			err = f.CreateBackup(backup)
			Expect(err).NotTo(HaveOccurred())

			By("Creating DaemonSet " + daemon.Name)
			_, err = f.CreateDaemonSet(daemon)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for sidecar")
			f.EventuallyDaemonSet(daemon.ObjectMeta).Should(HaveSidecar(util.StashContainer))

			By("Waiting for backup to complete")
			f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
				return r.Status.BackupCount
			}, BeNumerically(">=", 1)))

			By("Waiting for backup event")
			f.EventualEvent(backup.ObjectMeta).Should(WithTransform(f.CountSuccessfulBackups, BeNumerically(">=", 1)))
		}

		shouldBackupExistingDaemonSet = func() {
			By("Creating repository Secret " + cred.Name)
			err = f.CreateSecret(cred)
			Expect(err).NotTo(HaveOccurred())

			By("Creating DaemonSet " + daemon.Name)
			_, err = f.CreateDaemonSet(daemon)
			Expect(err).NotTo(HaveOccurred())

			By("Creating backup " + backup.Name)
			err = f.CreateBackup(backup)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for sidecar")
			f.EventuallyDaemonSet(daemon.ObjectMeta).Should(HaveSidecar(util.StashContainer))

			By("Waiting for backup to complete")
			f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
				return r.Status.BackupCount
			}, BeNumerically(">=", 1)))

			By("Waiting for backup event")
			f.EventualEvent(backup.ObjectMeta).Should(WithTransform(f.CountSuccessfulBackups, BeNumerically(">=", 1)))
		}

		shouldStopBackup = func() {
			By("Creating repository Secret " + cred.Name)
			err = f.CreateSecret(cred)
			Expect(err).NotTo(HaveOccurred())

			By("Creating backup " + backup.Name)
			err = f.CreateBackup(backup)
			Expect(err).NotTo(HaveOccurred())

			By("Creating DaemonSet " + daemon.Name)
			_, err = f.CreateDaemonSet(daemon)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for sidecar")
			f.EventuallyDaemonSet(daemon.ObjectMeta).Should(HaveSidecar(util.StashContainer))

			By("Waiting for backup to complete")
			f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
				return r.Status.BackupCount
			}, BeNumerically(">=", 1)))

			By("Deleting backup " + backup.Name)
			f.DeleteBackup(backup.ObjectMeta)

			f.EventuallyDaemonSet(daemon.ObjectMeta).ShouldNot(HaveSidecar(util.StashContainer))
		}

		shouldStopBackupIfLabelChanged = func() {
			By("Creating repository Secret " + cred.Name)
			err = f.CreateSecret(cred)
			Expect(err).NotTo(HaveOccurred())

			By("Creating backup " + backup.Name)
			err = f.CreateBackup(backup)
			Expect(err).NotTo(HaveOccurred())

			By("Creating DaemonSet " + daemon.Name)
			_, err = f.CreateDaemonSet(daemon)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for sidecar")
			f.EventuallyDaemonSet(daemon.ObjectMeta).Should(HaveSidecar(util.StashContainer))

			By("Waiting for backup to complete")
			f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
				return r.Status.BackupCount
			}, BeNumerically(">=", 1)))

			By("Removing labels of DaemonSet " + daemon.Name)
			_, _, err = ext_util.PatchDaemonSet(f.KubeClient, &daemon, func(in *extensions.DaemonSet) *extensions.DaemonSet {
				in.Labels = map[string]string{
					"app": "unmatched",
				}
				return in
			})
			Expect(err).NotTo(HaveOccurred())

			f.EventuallyDaemonSet(daemon.ObjectMeta).ShouldNot(HaveSidecar(util.StashContainer))
		}

		shouldStopBackupIfSelectorChanged = func() {
			By("Creating repository Secret " + cred.Name)
			err = f.CreateSecret(cred)
			Expect(err).NotTo(HaveOccurred())

			By("Creating backup " + backup.Name)
			err = f.CreateBackup(backup)
			Expect(err).NotTo(HaveOccurred())

			By("Creating DaemonSet " + daemon.Name)
			_, err = f.CreateDaemonSet(daemon)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for sidecar")
			f.EventuallyDaemonSet(daemon.ObjectMeta).Should(HaveSidecar(util.StashContainer))

			By("Waiting for backup to complete")
			f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
				return r.Status.BackupCount
			}, BeNumerically(">=", 1)))

			By("Change selector of Backup " + backup.Name)
			err = f.UpdateBackup(backup.ObjectMeta, func(in *api.Backup) *api.Backup {
				in.Spec.Selector = metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "unmatched",
					},
				}
				return in
			})
			Expect(err).NotTo(HaveOccurred())

			f.EventuallyDaemonSet(daemon.ObjectMeta).ShouldNot(HaveSidecar(util.StashContainer))
		}

		shouldRestoreDemonset = func() {
			shouldBackupNewDaemonSet()
			recovery.Spec.Workload = api.LocalTypedReference{
				Kind: api.KindDaemonSet,
				Name: daemon.Name,
			}
			recovery.Spec.NodeName = "minikube"

			By("Creating recovery " + recovery.Name)
			err = f.CreateRecovery(recovery)
			Expect(err).NotTo(HaveOccurred())

			f.EventuallyRecoverySucceed(recovery.ObjectMeta).Should(BeTrue())
		}

		shouldInitializeAndBackupDaemonSet = func() {
			By("Creating repository Secret " + cred.Name)
			err = f.CreateSecret(cred)
			Expect(err).NotTo(HaveOccurred())

			By("Creating backup " + backup.Name)
			err = f.CreateBackup(backup)
			Expect(err).NotTo(HaveOccurred())

			By("Creating DaemonSet " + daemon.Name)
			obj, err := f.CreateDaemonSet(daemon)
			Expect(err).NotTo(HaveOccurred())

			// sidecar should be added as soon as workload created, we don't need to wait for it
			By("Checking sidecar created")
			Expect(obj).Should(HaveSidecar(util.StashContainer))

			By("Waiting for backup to complete")
			f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
				return r.Status.BackupCount
			}, BeNumerically(">=", 1)))

			By("Waiting for backup event")
			f.EventualEvent(backup.ObjectMeta).Should(WithTransform(f.CountSuccessfulBackups, BeNumerically(">=", 1)))
		}
	)

	Describe("Creating backup for", func() {
		AfterEach(func() {
			f.DeleteDaemonSet(daemon.ObjectMeta)
			f.DeleteBackup(backup.ObjectMeta)
			f.DeleteSecret(cred.ObjectMeta)
		})

		Context(`"Local" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForLocalBackend()
				backup = f.BackupForLocalBackend()
			})
			It(`should backup new DaemonSet`, shouldBackupNewDaemonSet)
			It(`should backup existing DaemonSet`, shouldBackupExistingDaemonSet)
		})

		Context(`"S3" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForS3Backend()
				backup = f.BackupForS3Backend()
			})
			It(`should backup new DaemonSet`, shouldBackupNewDaemonSet)
			It(`should backup existing DaemonSet`, shouldBackupExistingDaemonSet)
		})

		Context(`"DO" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForDOBackend()
				backup = f.BackupForDOBackend()
			})
			It(`should backup new DaemonSet`, shouldBackupNewDaemonSet)
			It(`should backup existing DaemonSet`, shouldBackupExistingDaemonSet)
		})

		Context(`"GCS" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForGCSBackend()
				backup = f.BackupForGCSBackend()
			})
			It(`should backup new DaemonSet`, shouldBackupNewDaemonSet)
			It(`should backup existing DaemonSet`, shouldBackupExistingDaemonSet)
		})

		Context(`"Azure" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForAzureBackend()
				backup = f.BackupForAzureBackend()
			})
			It(`should backup new DaemonSet`, shouldBackupNewDaemonSet)
			It(`should backup existing DaemonSet`, shouldBackupExistingDaemonSet)
		})

		Context(`"Swift" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForSwiftBackend()
				backup = f.BackupForSwiftBackend()
			})
			It(`should backup new DaemonSet`, shouldBackupNewDaemonSet)
			It(`should backup existing DaemonSet`, shouldBackupExistingDaemonSet)
		})

		Context(`"B2" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForB2Backend()
				backup = f.BackupForB2Backend()
			})
			It(`should backup new DaemonSet`, shouldBackupNewDaemonSet)
			It(`should backup existing DaemonSet`, shouldBackupExistingDaemonSet)
		})
	})

	Describe("Changing DaemonSet labels", func() {
		AfterEach(func() {
			f.DeleteDaemonSet(daemon.ObjectMeta)
			f.DeleteBackup(backup.ObjectMeta)
			f.DeleteSecret(cred.ObjectMeta)
		})
		BeforeEach(func() {
			cred = f.SecretForLocalBackend()
			backup = f.BackupForLocalBackend()
		})
		It(`should stop backup`, shouldStopBackupIfLabelChanged)
	})

	Describe("Changing Backup selector", func() {
		AfterEach(func() {
			f.DeleteDaemonSet(daemon.ObjectMeta)
			f.DeleteBackup(backup.ObjectMeta)
			f.DeleteSecret(cred.ObjectMeta)
		})
		BeforeEach(func() {
			cred = f.SecretForLocalBackend()
			backup = f.BackupForLocalBackend()
		})
		It(`should stop backup`, shouldStopBackupIfSelectorChanged)
	})

	Describe("Deleting backup for", func() {
		AfterEach(func() {
			f.DeleteDaemonSet(daemon.ObjectMeta)
			f.DeleteSecret(cred.ObjectMeta)
		})

		Context(`"Local" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForLocalBackend()
				backup = f.BackupForLocalBackend()
			})
			It(`should stop backup`, shouldStopBackup)
		})

		Context(`"S3" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForS3Backend()
				backup = f.BackupForS3Backend()
			})
			It(`should stop backup`, shouldStopBackup)
		})

		Context(`"DO" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForDOBackend()
				backup = f.BackupForDOBackend()
			})
			It(`should stop backup`, shouldStopBackup)
		})

		Context(`"GCS" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForGCSBackend()
				backup = f.BackupForGCSBackend()
			})
			It(`should stop backup`, shouldStopBackup)
		})

		Context(`"Azure" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForAzureBackend()
				backup = f.BackupForAzureBackend()
			})
			It(`should stop backup`, shouldStopBackup)
		})

		Context(`"Swift" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForSwiftBackend()
				backup = f.BackupForSwiftBackend()
			})
			It(`should stop backup`, shouldStopBackup)
		})

		Context(`"B2" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForB2Backend()
				backup = f.BackupForB2Backend()
			})
			It(`should stop backup`, shouldStopBackup)
		})
	})

	Describe("Creating recovery for", func() {
		AfterEach(func() {
			f.DeleteDaemonSet(daemon.ObjectMeta)
			f.DeleteBackup(backup.ObjectMeta)
			f.DeleteSecret(cred.ObjectMeta)
			f.DeleteRecovery(recovery.ObjectMeta)
			framework.CleanupMinikubeHostPath()
		})

		Context(`"Local" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForLocalBackend()
				backup = f.BackupForHostPathLocalBackend()
				recovery = f.RecoveryForBackup(backup)
			})
			It(`should restore local daemonset backup`, shouldRestoreDemonset)
		})

		Context(`"S3" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForS3Backend()
				backup = f.BackupForS3Backend()
				recovery = f.RecoveryForBackup(backup)
			})
			It(`should restore s3 daemonset backup`, shouldRestoreDemonset)
		})
	})

	Describe("Stash initializer for", func() {
		AfterEach(func() {
			f.DeleteDaemonSet(daemon.ObjectMeta)
			f.DeleteBackup(backup.ObjectMeta)
			f.DeleteSecret(cred.ObjectMeta)
		})

		Context(`"Local" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForLocalBackend()
				backup = f.BackupForLocalBackend()
			})
			It("should initialize and backup new DaemonSet", shouldInitializeAndBackupDaemonSet)
		})
	})

	Describe("Offline backup for", func() {
		AfterEach(func() {
			f.DeleteDaemonSet(daemon.ObjectMeta)
			f.DeleteBackup(backup.ObjectMeta)
			f.DeleteSecret(cred.ObjectMeta)
			framework.CleanupMinikubeHostPath()
		})

		Context(`"Local" backend`, func() {
			BeforeEach(func() {
				cred = f.SecretForLocalBackend()
				backup = f.BackupForHostPathLocalBackend()
				backup.Spec.Type = api.BackupOffline
				backup.Spec.Schedule = "*/5 * * * *"
			})
			It(`should backup new DaemonSet`, func() {
				By("Creating repository Secret " + cred.Name)
				err = f.CreateSecret(cred)
				Expect(err).NotTo(HaveOccurred())

				By("Creating backup " + backup.Name)
				err = f.CreateBackup(backup)
				Expect(err).NotTo(HaveOccurred())

				cronJobName := util.KubectlCronPrefix + backup.Name
				By("Checking cron job created: " + cronJobName)
				Eventually(func() error {
					_, err := f.KubeClient.BatchV1beta1().CronJobs(backup.Namespace).Get(cronJobName, metav1.GetOptions{})
					return err
				}).Should(BeNil())

				By("Creating DaemonSet " + daemon.Name)
				_, err = f.CreateDaemonSet(daemon)
				Expect(err).NotTo(HaveOccurred())

				By("Waiting for init-container")
				f.EventuallyDaemonSet(daemon.ObjectMeta).Should(HaveInitContainer(util.StashContainer))

				By("Waiting for initial backup to complete")
				f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
					return r.Status.BackupCount
				}, BeNumerically(">=", 1)))

				By("Waiting for next backup to complete")
				f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
					return r.Status.BackupCount
				}, BeNumerically(">=", 2)))

				By("Waiting for backup event")
				f.EventualEvent(backup.ObjectMeta).Should(WithTransform(f.CountSuccessfulBackups, BeNumerically(">", 1)))
			})
		})
	})

	Describe("Pause Backup to stop backup", func() {
		Context(`"Local" backend`, func() {
			AfterEach(func() {
				f.DeleteDaemonSet(daemon.ObjectMeta)
				f.DeleteBackup(backup.ObjectMeta)
				f.DeleteSecret(cred.ObjectMeta)
			})
			BeforeEach(func() {
				cred = f.SecretForLocalBackend()
				backup = f.BackupForLocalBackend()
			})
			It(`should be able to Pause and Resume backup`, func() {
				By("Creating repository Secret " + cred.Name)
				err = f.CreateSecret(cred)
				Expect(err).NotTo(HaveOccurred())

				By("Creating backup")
				err = f.CreateBackup(backup)
				Expect(err).NotTo(HaveOccurred())

				By("Creating Daemonset " + daemon.Name)
				_, err = f.CreateDaemonSet(daemon)
				Expect(err).NotTo(HaveOccurred())

				By("Waiting for sidecar")
				f.EventuallyDaemonSet(daemon.ObjectMeta).Should(HaveSidecar(util.StashContainer))

				By("Waiting for backup to complete")
				f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
					return r.Status.BackupCount
				}, BeNumerically(">=", 1)))

				By("Waiting for backup event")
				f.EventualEvent(backup.ObjectMeta).Should(WithTransform(f.CountSuccessfulBackups, BeNumerically(">=", 1)))

				By(`Patching Backup with "paused: true"`)
				err = f.CreateOrPatchBackup(backup.ObjectMeta, func(in *api.Backup) *api.Backup {
					in.Spec.Paused = true
					return in
				})
				Expect(err).NotTo(HaveOccurred())

				backupObj, err := f.StashClient.StashV1alpha1().Backups(backup.Namespace).Get(backup.Name, metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred())

				previousBackupCount := backupObj.Status.BackupCount

				By("Wating 2 minutes")
				time.Sleep(2 * time.Minute)

				By("Checking that Backup count has not changed")
				backupObj, err = f.StashClient.StashV1alpha1().Backups(backup.Namespace).Get(backup.Name, metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred())
				Expect(backupObj.Status.BackupCount).Should(BeNumerically("==", previousBackupCount))

				By(`Patching Backup with "paused: false"`)
				err = f.CreateOrPatchBackup(backup.ObjectMeta, func(in *api.Backup) *api.Backup {
					in.Spec.Paused = false
					return in
				})
				Expect(err).NotTo(HaveOccurred())

				By("Waiting for backup to complete")
				f.EventuallyBackup(backup.ObjectMeta).Should(WithTransform(func(r *api.Backup) int64 {
					return r.Status.BackupCount
				}, BeNumerically(">", previousBackupCount)))

				By("Waiting for backup event")
				f.EventualEvent(backup.ObjectMeta).Should(WithTransform(f.CountSuccessfulBackups, BeNumerically(">", previousBackupCount)))

			})

		})
	})
})
