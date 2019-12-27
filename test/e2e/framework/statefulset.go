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

package framework

import (
	"fmt"

	"stash.appscode.dev/stash/apis"

	"github.com/appscode/go/crypto/rand"
	"github.com/appscode/go/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kutil "kmodules.xyz/client-go"
	apps_util "kmodules.xyz/client-go/apps/v1"
)

func (fi *Invocation) StatefulSet(pvcName, volName string) apps.StatefulSet {
	labels := map[string]string{
		"app":  fi.app,
		"kind": "statefulset",
	}
	return apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rand.WithUniqSuffix("stash"),
			Namespace: fi.namespace,
			Labels:    labels,
		},
		Spec: apps.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Replicas:    types.Int32P(1),
			Template:    fi.PodTemplate(labels, pvcName, volName),
			ServiceName: TEST_HEADLESS_SERVICE,
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
			},
		},
	}
}

func (fi *Invocation) StatefulSetForV1beta1API(name, volName string, replica int32) apps.StatefulSet {
	labels := map[string]string{
		"app":  fi.app,
		"kind": "statefulset",
	}
	return apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: fi.namespace,
			Labels:    labels,
		},
		Spec: apps.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Replicas:    &replica,
			ServiceName: TEST_HEADLESS_SERVICE,
			Template: core.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: core.PodSpec{
					Containers: []core.Container{
						{
							Name:            "busybox",
							Image:           "busybox",
							ImagePullPolicy: core.PullIfNotPresent,
							Command: []string{
								"sleep",
								"3600",
							},
							VolumeMounts: []core.VolumeMount{
								{
									Name:      volName,
									MountPath: TestSourceDataMountPath,
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []core.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: volName,
					},
					Spec: core.PersistentVolumeClaimSpec{
						AccessModes: []core.PersistentVolumeAccessMode{
							core.ReadWriteOnce,
						},
						StorageClassName: types.StringP(fi.StorageClass),
						Resources: core.ResourceRequirements{
							Requests: core.ResourceList{
								core.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		},
	}
}

func (f *Framework) CreateStatefulSet(obj apps.StatefulSet) (*apps.StatefulSet, error) {
	return f.KubeClient.AppsV1().StatefulSets(obj.Namespace).Create(&obj)
}

func (f *Framework) DeleteStatefulSet(meta metav1.ObjectMeta) error {
	err := f.KubeClient.AppsV1().StatefulSets(meta.Namespace).Delete(meta.Name, deleteInBackground())
	if err != nil && !kerr.IsNotFound(err) {
		return err
	}
	return nil
}

func (f *Framework) EventuallyStatefulSet(meta metav1.ObjectMeta) GomegaAsyncAssertion {
	return Eventually(func() *apps.StatefulSet {
		obj, err := f.KubeClient.AppsV1().StatefulSets(meta.Namespace).Get(meta.Name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		return obj
	})
}

func (f *Invocation) WaitUntilStatefulSetReadyWithSidecar(meta metav1.ObjectMeta) error {
	return wait.PollImmediate(kutil.RetryInterval, kutil.ReadinessTimeout, func() (bool, error) {
		if obj, err := f.KubeClient.AppsV1().StatefulSets(meta.Namespace).Get(meta.Name, metav1.GetOptions{}); err == nil {
			if obj.Status.Replicas == obj.Status.ReadyReplicas {
				pods, err := f.GetAllPods(obj.ObjectMeta)
				if err != nil {
					return false, err
				}

				for i := range pods {
					hasSidecar := false
					for _, c := range pods[i].Spec.Containers {
						if c.Name == apis.StashContainer {
							hasSidecar = true
						}
					}
					if !hasSidecar {
						return false, nil
					}
				}
				return true, nil
			}
			return false, nil
		}
		return false, nil
	})
}

func (f *Invocation) WaitUntilStatefulSetWithInitContainer(meta metav1.ObjectMeta) error {
	return wait.PollImmediate(kutil.RetryInterval, kutil.ReadinessTimeout, func() (bool, error) {
		if obj, err := f.KubeClient.AppsV1().StatefulSets(meta.Namespace).Get(meta.Name, metav1.GetOptions{}); err == nil {
			if obj.Status.Replicas == obj.Status.ReadyReplicas {
				pods, err := f.GetAllPods(obj.ObjectMeta)
				if err != nil {
					return false, err
				}

				for i := range pods {
					hasInitContainer := false
					for _, c := range pods[i].Spec.InitContainers {
						if c.Name == apis.StashInitContainer {
							hasInitContainer = true
						}
					}
					if !hasInitContainer {
						return false, nil
					}
				}
				return true, nil
			}
			return false, nil
		}
		return false, nil
	})
}

func (f *Invocation) DeployStatefulSet(name string, replica int32, volName string, transformFuncs ...func(ss *apps.StatefulSet)) (*apps.StatefulSet, error) {
	// append test case specific suffix so that name does not conflict during parallel test
	name = fmt.Sprintf("%s-%s", name, f.app)

	// Generate StatefulSet definition
	ss := f.StatefulSetForV1beta1API(name, volName, replica)

	// transformFuncs provides a array of functions that made test specific change on the StatefulSet
	// apply these test specific changes
	for _, fn := range transformFuncs {
		fn(&ss)
	}

	By("Deploying StatefulSet: " + ss.Name)
	createdss, err := f.CreateStatefulSet(ss)
	if err != nil {
		return createdss, err
	}
	f.AppendToCleanupList(createdss)

	By("Waiting for StatefulSet to be ready")
	err = apps_util.WaitUntilStatefulSetReady(f.KubeClient, createdss.ObjectMeta)
	Expect(err).NotTo(HaveOccurred())
	// check that we can execute command to the pod.
	// this is necessary because we will exec into the pods and create sample data
	f.EventuallyPodAccessible(createdss.ObjectMeta).Should(BeTrue())

	return createdss, err
}

func (f *Invocation) DeployStatefulSetWithProbeClient() (*apps.StatefulSet, error) {
	svc, err := f.CreateService(f.HeadlessService())
	if err != nil {
		return nil, err
	}
	f.AppendToCleanupList(svc)

	labels := map[string]string{
		"app":  f.app,
		"kind": "statefulset",
	}
	// Generate StatefulSet definition
	statefulset := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", ProberDemoPodPrefix, f.app),
			Namespace: f.namespace,
		},
		Spec: apps.StatefulSetSpec{
			Replicas: types.Int32P(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			ServiceName: TEST_HEADLESS_SERVICE,
			Template: core.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: core.PodSpec{
					Containers: []core.Container{
						{
							Name:  ProberDemoPodPrefix,
							Image: "appscodeci/prober-demo",
							Args: []string{
								"run-client",
							},
							Env: []core.EnvVar{
								{
									Name:  ExitCodeSuccess,
									Value: "0",
								},
								{
									Name:  ExitCodeFail,
									Value: "1",
								},
							},
							Ports: []core.ContainerPort{
								{
									Name:          HttpPortName,
									ContainerPort: HttpPort,
								},
								{
									Name:          TcpPortName,
									ContainerPort: TcpPort,
								},
							},
							VolumeMounts: []core.VolumeMount{
								{
									Name:      SourceVolume,
									MountPath: TestSourceDataMountPath,
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []core.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: SourceVolume,
					},
					Spec: core.PersistentVolumeClaimSpec{
						AccessModes: []core.PersistentVolumeAccessMode{
							core.ReadWriteOnce,
						},
						StorageClassName: types.StringP(f.StorageClass),
						Resources: core.ResourceRequirements{
							Requests: core.ResourceList{
								core.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		},
	}

	By("Deploying StatefulSet with Probe Client: " + statefulset.Name)
	createdStatefulSet, err := f.CreateStatefulSet(*statefulset)
	if err != nil {
		return createdStatefulSet, err
	}
	f.AppendToCleanupList(createdStatefulSet)

	By("Waiting for StatefulSet to be ready")
	err = apps_util.WaitUntilStatefulSetReady(f.KubeClient, createdStatefulSet.ObjectMeta)
	Expect(err).NotTo(HaveOccurred())
	// check that we can execute command to the pod.
	// this is necessary because we will exec into the pods and create sample data
	f.EventuallyPodAccessible(createdStatefulSet.ObjectMeta).Should(BeTrue())

	return createdStatefulSet, err
}
