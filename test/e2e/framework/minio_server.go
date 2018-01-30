package framework

import (
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/appscode/go/crypto/rand"
	"github.com/appscode/stash/pkg/cli"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	apps "k8s.io/api/apps/v1beta1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	MINIO_PUBLIC_CRT  = "MINIO_PUBLIC_CRT"
	MINIO_PRIVATE_KEY = "MINIO_PRIVATE_KEY"

	MINIO_PUBLIC_CRT_NAME  = "public.crt"
	MINIO_PRIVSTE_KEY_NAME = "private.key"
	MINIO_CA_CRT           = "MINIO_CA_CRT"
)

var (
	mcred   core.Secret
	mpvc    core.PersistentVolumeClaim
	mdeploy apps.Deployment
	msrvc   core.Service
)

func (fi *Invocation) CreateMinioServer() (string, error) {
	//creating secret for minio server
	mcred = fi.SecretForMinioServer()
	err := fi.CreateSecret(mcred)
	if err != nil {
		return "", err
	}

	//creating pvc for minio server
	mpvc = fi.PVCForMinioServer()
	err = fi.CreatePersistentVolumeClaim(mpvc)
	if err != nil {
		return "", nil
	}

	//creating deployment for minio server
	mdeploy = fi.DeploymentForMinioServer()
	err = fi.CreateDeploymentForMinioServer(mdeploy)
	if err != nil {
		return "", err
	}

	//creating service for minio server
	msrvc = fi.ServiceForMinioServer()
	srvc, err := fi.CreateServiceForMinioServer(msrvc)
	if err != nil {
		return "", err
	}

	addr := fi.getMinioServerAddress(srvc)

	return addr, nil
}
func (fi *Invocation) SecretForMinioServer() core.Secret {
	if os.Getenv(MINIO_PUBLIC_CRT) == "" || os.Getenv(MINIO_PRIVATE_KEY) == "" {
		return core.Secret{}
	}
	crtData, crt_err := ioutil.ReadFile(os.Getenv(MINIO_PUBLIC_CRT))
	keyData, key_err := ioutil.ReadFile(os.Getenv(MINIO_PRIVATE_KEY))

	if crt_err != nil && key_err != nil {
		return core.Secret{}
	}

	return core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio-server-secret",
			Namespace: fi.namespace,
		},
		Data: map[string][]byte{
			MINIO_PUBLIC_CRT_NAME:  crtData,
			MINIO_PRIVSTE_KEY_NAME: keyData,
		},
	}
}

func (fi *Invocation) PVCForMinioServer() core.PersistentVolumeClaim {
	return core.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio-pv-claim",
			Namespace: fi.namespace,
			Labels: map[string]string{
				// this label will be used to mount this pvc as volume in minio server container
				"app": "minio-storage-claim",
			},
		},
		Spec: core.PersistentVolumeClaimSpec{
			AccessModes: []core.PersistentVolumeAccessMode{
				core.ReadWriteOnce,
			},
			Resources: core.ResourceRequirements{
				Requests: core.ResourceList{
					core.ResourceName(core.ResourceStorage): resource.MustParse("2Gi"),
				},
			},
		},
	}
}

func (fi *Invocation) CreatePersistentVolumeClaim(obj core.PersistentVolumeClaim) error {
	_, err := fi.KubeClient.CoreV1().PersistentVolumeClaims(obj.Namespace).Create(&obj)
	return err
}

func (fi *Invocation) DeploymentForMinioServer() apps.Deployment {
	return apps.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix("minio-server"),
			Namespace: fi.namespace,
		},
		Spec: apps.DeploymentSpec{
			Strategy: apps.DeploymentStrategy{
				Type: apps.RecreateDeploymentStrategyType,
			},
			Template: core.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					// minio service will select this pod using this label.
					Labels: map[string]string{
						"app": fi.app + "minio-server",
					},
				},
				Spec: core.PodSpec{
					// this volumes will be mounted on minio server container
					Volumes: []core.Volume{
						{
							Name: "minio-storage",
							VolumeSource: core.VolumeSource{
								PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
									ClaimName: "minio-pv-claim",
								},
							},
						},
						{
							Name: "minio-secret",
							VolumeSource: core.VolumeSource{
								Secret: &core.SecretVolumeSource{
									SecretName: "minio-server-secret",
								},
							},
						},
					},
					// run this containers in minio server pod
					Containers: []core.Container{
						{
							Name:  "minio-server",
							Image: "minio/minio",
							Args: []string{
								"server",
								"--address",
								":443",
								"/storage",
							},
							Env: []core.EnvVar{
								{
									Name:  "MINIO_ACCESS_KEY",
									Value: os.Getenv(cli.AWS_ACCESS_KEY_ID),
								},
								{
									Name:  "MINIO_SECRET_KEY",
									Value: os.Getenv(cli.AWS_SECRET_ACCESS_KEY),
								},
							},
							Ports: []core.ContainerPort{
								{
									ContainerPort: int32(443),
									HostPort:      int32(443),
								},
							},
							VolumeMounts: []core.VolumeMount{
								{
									Name:      "minio-storage",
									MountPath: "/storage",
								},
								{
									Name:      "minio-secret",
									MountPath: "/root/.minio/certs/",
								},
							},
						},
					},
				},
			},
		},
	}
}

func (fi *Invocation) CreateDeploymentForMinioServer(obj apps.Deployment) error {
	_, err := fi.KubeClient.AppsV1beta1().Deployments(obj.Namespace).Create(&obj)
	if err == nil {
		//wating 30 second to minio server to be ready
		time.Sleep(time.Second * 30)
	}
	return err
}

func (fi *Invocation) ServiceForMinioServer() core.Service {
	return core.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "minio-service",
			Namespace: fi.namespace,
		},
		Spec: core.ServiceSpec{
			Type: core.ServiceTypeLoadBalancer,
			Ports: []core.ServicePort{
				{
					Port:       int32(443),
					TargetPort: intstr.FromInt(443),
					Protocol:   core.ProtocolTCP,
				},
			},
			Selector: map[string]string{
				"app": fi.app + "minio-server",
			},
		},
	}
}

func (fi *Invocation) CreateServiceForMinioServer(obj core.Service) (*core.Service, error) {
	return fi.KubeClient.CoreV1().Services(obj.Namespace).Create(&obj)
}

func (fi *Invocation) getMinioServerAddress(svc *core.Service) string {
	// Assuming that test is running on minikube
	return "192.168.99.100:" + strconv.Itoa(int(svc.Spec.Ports[0].NodePort))
}

func (fi *Invocation) DeleteMinioServer() {
	fi.DeleteSecretForMinioServer(mcred.ObjectMeta)
	fi.DeletePVCForMinioServer(mpvc.ObjectMeta)
	fi.DeleteDeploymentForMinioServer(mdeploy.ObjectMeta)
	fi.DeleteServiceForMinioServer(msrvc.ObjectMeta)
}
func (f *Framework) DeleteSecretForMinioServer(meta metav1.ObjectMeta) error {
	return f.KubeClient.CoreV1().Secrets(meta.Namespace).Delete(meta.Name, deleteInForeground())
}

func (f *Framework) DeletePVCForMinioServer(meta metav1.ObjectMeta) error {
	return f.KubeClient.CoreV1().PersistentVolumeClaims(meta.Namespace).Delete(meta.Name, deleteInForeground())
}

func (f *Framework) DeleteDeploymentForMinioServer(meta metav1.ObjectMeta) error {
	return f.KubeClient.AppsV1beta1().Deployments(meta.Namespace).Delete(meta.Name, deleteInForeground())
}

func (f *Framework) DeleteServiceForMinioServer(meta metav1.ObjectMeta) error {
	return f.KubeClient.CoreV1().Services(meta.Namespace).Delete(meta.Name, deleteInForeground())
}
