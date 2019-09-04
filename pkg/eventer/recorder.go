package eventer

import (
	"fmt"
	"time"

	"github.com/appscode/go/log"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
)

const (
	EventReasonInvalidRestic                 = "InvalidRestic"
	EventReasonInvalidRecovery               = "InvalidRecovery"
	EventReasonInvalidCronExpression         = "InvalidCronExpression"
	EventReasonSuccessfulCronExpressionReset = "SuccessfulCronExpressionReset"
	EventReasonFailedToBackup                = "FailedBackup"
	EventReasonSuccessfulRecovery            = "SuccessfulRecovery"
	EventReasonFailedToRecover               = "FailedRecovery"
	EventReasonSuccessfulCheck               = "SuccessfulCheck"
	EventReasonFailedToCheck                 = "FailedCheck"
	EventReasonFailedToRetention             = "FailedRetention"
	EventReasonFailedToUpdate                = "FailedUpdateBackup"
	EventReasonFailedCronJob                 = "FailedCronJob"
	EventReasonFailedToDelete                = "FailedDelete"
	EventReasonJobCreated                    = "RecoveryJobCreated"
	EventReasonJobFailedToCreate             = "RecoveryJobFailedToCreate"
	EventReasonCheckJobCreated               = "CheckJobCreated"
	EventReasonFailedSetup                   = "SetupFailed"
	EventReasonAdmissionWebhookNotActivated  = "AdmissionWebhookNotActivated"
	EventReasonInvalidBackupConfiguration    = "InvalidBackupConfiguration"

	EventReasonInvalidBackupSession = "InvalidBackupSession"
	EventReasonHostBackupSucceded   = "SuccessfulHostBackup"
	EventReasonHostBackupFailed     = "FailedHostBackup"

	EventReasonInvalidRestoreSession   = "InvalidRestoreSession"
	EventReasonRestoreSessionSucceeded = "RestoreSessionSucceeded"
	EventReasonRestoreSessionFailed    = "RestoreSessionFailedToExecute"
	EventReasonRestorePhaseUnknown     = "RestoreSession Phase Unknown"
	EventReasonRestoreJobCreated       = "RestoreJobCreated"

	// RestoreSession events
	EventReasonRestoreFailed        = "FailedRestoreSession"
	EventReasonRestoreSucceded      = "SuccessfulRestoreSession"
	EventReasonHostRestoreSucceeded = "SuccessfulHostRestore"
	EventReasonHostRestoreFailed    = "FailedHostRestore"

	// ====================== Event Sources ===================================
	EventSourceBackupConfigurationController = "BackupConfiguration Controller"
	EventSourceBackupSessionController       = "BackupSession Controller"
	EventSourceRestoreSessionController      = "RestoreSession Controller"
	EventSourceBackupSidecar                 = "Backup Sidecar"
	EventSourceRestoreInitContainer          = "Restore Init-Container"
	EventSourceBackupTriggeringCronJob       = "Backup Triggering CronJob"
	EventSourcePostBackupStatusUpdater       = "Post Backup Status Updater"
	EventSourcePostRestoreStatusUpdater      = "Post Restore Status Updater"
	EventSourceAutoBackupHandler             = "Auto Backup Handler"

	// ======================= Event Reasons ========================
	// BackupConfiguration Events
	EventReasonCronJobCreationFailed   = "CronJob Creation Failed"
	EventReasonSidecarInjectionFailed  = "Sidecar Injection Failed"
	EventReasonBackupJobCreationFailed = "Backup Job Creation Failed"
	// BackupSession Events
	EventReasonBackupSessionFailed    = "BackupSession Failed"
	EventReasonBackupSessionSkipped   = "BackupSession Skipped"
	EventReasonBackupSessionRunning   = "BackupSession Running"
	EventReasonBackupSessionSucceeded = "BackupSession Succeeded"
	// RestoreSession Events
	EventReasonInitContainerInjectionFailed  = "Init-Container Injection Failed"
	EventReasonRestoreJobCreationFailed = "Restore Job Creation Failed"
	// Auto Backup Events
	EventReasonAutoBackupResourcesCreationFailed    = "Auto Backup Resources Creation Failed"
	EventReasonAutoBackupResourcesCreationSucceeded = "Auto Backup Resources Creation Succeeded"
	EventReasonAutoBackupResourcesDeletionFailed    = "Auto Backup Resources Deletion Failed"
	EventReasonAutoBackupResourcesDeletionSucceeded = "Auto Backup Resources Deletion Succeeded"

	EventReasonBackupSkipped = "Backup Skipped"
)

func NewEventRecorder(client kubernetes.Interface, component string) record.EventRecorder {
	// Event Broadcaster
	broadcaster := record.NewBroadcaster()
	broadcaster.StartEventWatcher(
		func(event *core.Event) {
			if _, err := client.CoreV1().Events(event.Namespace).Create(event); err != nil {
				log.Errorln(err)
			}
		},
	)
	// Event Recorder
	return broadcaster.NewRecorder(scheme.Scheme, core.EventSource{Component: component})
}

func CreateEvent(client kubernetes.Interface, component string, obj runtime.Object, eventType, reason, message string) (*core.Event, error) {
	ref, err := reference.GetReference(scheme.Scheme, obj)
	if err != nil {
		return nil, err
	}

	t := metav1.Time{Time: time.Now()}

	return client.CoreV1().Events(ref.Namespace).Create(&core.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%v.%x", ref.Name, t.UnixNano()),
			Namespace: ref.Namespace,
		},
		InvolvedObject: *ref,
		Reason:         reason,
		Message:        message,
		FirstTimestamp: t,
		LastTimestamp:  t,
		Count:          1,
		Type:           eventType,
		Source:         core.EventSource{Component: component},
	})
}

func CreateEventWithLog(client kubernetes.Interface, component string, obj runtime.Object, eventType, reason, message string) {
	event, err := CreateEvent(client, component, obj, eventType, reason, message)
	if err != nil {
		log.Errorln("Failed to write event, reason: ", err)
	} else {
		log.Infoln("Event created: ", event.Name)
	}
}
