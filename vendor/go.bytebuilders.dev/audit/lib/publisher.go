/*
Copyright AppsCode Inc. and Contributors

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

package lib

import (
	"context"
	"fmt"
	"time"

	api "go.bytebuilders.dev/audit/api/v1"

	cloudeventssdk "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
	"gomodules.xyz/sync"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"kmodules.xyz/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type EventCreator func(obj runtime.Object) (*api.Event, error)

type EventPublisher struct {
	once    sync.Once
	connect func() error

	nats        *NatsConfig
	mapper      discovery.ResourceMapper
	createEvent EventCreator
}

func NewEventPublisher(
	nats *NatsConfig,
	mapper discovery.ResourceMapper,
	fn EventCreator,
) *EventPublisher {
	p := &EventPublisher{
		mapper:      mapper,
		createEvent: fn,
	}
	p.connect = func() error {
		p.nats = nats
		return nil
	}
	return p
}

func NewResilientEventPublisher(
	fnConnect func() (*NatsConfig, error),
	mapper discovery.ResourceMapper,
	fnCreateEvent EventCreator,
) *EventPublisher {
	p := &EventPublisher{
		mapper:      mapper,
		createEvent: fnCreateEvent,
	}
	p.connect = func() error {
		var err error
		p.nats, err = fnConnect()
		if err != nil {
			klog.V(5).InfoS("failed to connect with event receiver", "error", err)
		}
		return err
	}
	return p
}

func (p *EventPublisher) Publish(ev *api.Event, et api.EventType) error {
	event := cloudeventssdk.NewEvent()
	setEventDefaults(&event, p.nats.Subject, et)

	if err := event.SetData(cloudevents.ApplicationJSON, ev); err != nil {
		return err
	}

	data, err := format.JSON.Marshal(&event)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.TODO(), natsEventPublishTimeout)
	defer cancel()

	for {
		_, err = p.nats.Client.Request(p.nats.Subject, data, natsRequestTimeout)
		if err == nil {
			cancel()
		} else {
			klog.V(5).Infoln(err)
		}

		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				klog.V(5).Infof("failed to send event : %s", string(data))
			} else if ctx.Err() == context.Canceled {
				klog.V(5).Infof("Published event `%s` to channel `%s` and acknowledged", et, p.nats.Subject)
			}
			return nil
		default:
			time.Sleep(time.Microsecond * 100)
		}
	}
}

func setEventDefaults(event *cloudevents.Event, natsSubject string, et api.EventType) {
	event.SetID(uuid.New().String())
	event.SetSubject(natsSubject)
	event.SetType(string(et))
	event.SetSource("kubeops.dev/auditor")
	event.SetTime(time.Now())
}

func (p *EventPublisher) ForGVK(gvk schema.GroupVersionKind) cache.ResourceEventHandler {
	if gvk.Version == "" || gvk.Kind == "" {
		panic(fmt.Sprintf("incomplete GVK; %+v", gvk))
	}

	return &ResourceEventPublisher{
		p: p,
		createEvent: func(obj runtime.Object) (*api.Event, error) {
			r := obj.DeepCopyObject()
			r.GetObjectKind().SetGroupVersionKind(gvk)
			m, err := meta.Accessor(r)
			if err != nil {
				return nil, err
			}
			m.SetManagedFields(nil)

			ev, err := p.createEvent(r)
			if err != nil {
				return nil, err
			}

			p.once.Do(p.connect)
			if p.nats == nil {
				return nil, fmt.Errorf("not connected to nats")
			}
			ev.LicenseID = p.nats.LicenseID

			return ev, nil
		},
	}
}

func (p *EventPublisher) SetupWithManagerForKind(ctx context.Context, mgr manager.Manager, gvk schema.GroupVersionKind) error {
	if p == nil {
		return nil
	}
	i, err := mgr.GetCache().GetInformerForKind(ctx, gvk)
	if err != nil {
		return err
	}
	i.AddEventHandler(p.ForGVK(gvk))
	return nil
}

func (p *EventPublisher) SetupWithManager(ctx context.Context, mgr manager.Manager, obj client.Object) error {
	gvk, err := apiutil.GVKForObject(obj, mgr.GetScheme())
	if err != nil {
		return err
	}
	return p.SetupWithManagerForKind(ctx, mgr, gvk)
}

type ResourceEventPublisher struct {
	p           *EventPublisher
	createEvent EventCreator
}

var _ cache.ResourceEventHandler = &ResourceEventPublisher{}

func (p *ResourceEventPublisher) OnAdd(o interface{}) {
	obj, ok := o.(runtime.Object)
	if !ok {
		return
	}

	ev, err := p.createEvent(obj)
	if err != nil {
		klog.V(5).InfoS("failed to create event data", "error", err)
		return
	}

	if err = p.p.Publish(ev, api.EventCreate); err != nil {
		klog.V(5).InfoS("error while publishing event", "error", err)
	}
}

func (p *ResourceEventPublisher) OnUpdate(oldObj, newObj interface{}) {
	uOld, err := meta.Accessor(oldObj)
	if err != nil {
		klog.V(5).InfoS("failed to get accessor for old object", "error", err)
		return
	}
	uNew, err := meta.Accessor(newObj)
	if err != nil {
		klog.V(5).InfoS("failed to get accessor for new object", "error", err)
		return
	}

	obj, ok := newObj.(runtime.Object)
	if !ok {
		return
	}

	if uOld.GetUID() == uNew.GetUID() && uOld.GetGeneration() == uNew.GetGeneration() {
		if klog.V(8).Enabled() {
			klog.V(8).InfoS("skipping update event",
				"gvk", obj.GetObjectKind().GroupVersionKind(),
				"namespace", uNew.GetNamespace(),
				"name", uNew.GetName(),
			)
		}
		return
	}

	ev, err := p.createEvent(obj)
	if err != nil {
		klog.V(5).InfoS("failed to create event data", "error", err)
		return
	}

	if err = p.p.Publish(ev, api.EventUpdate); err != nil {
		klog.V(5).InfoS("failed to publish event", "error", err)
	}
}

func (p *ResourceEventPublisher) OnDelete(obj interface{}) {
	var object runtime.Object
	var ok bool
	if object, ok = obj.(runtime.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			klog.V(5).Info("error decoding object, invalid type")
			return
		}
		object, ok = tombstone.Obj.(runtime.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		klog.V(5).Infof("Recovered deleted object '%v' from tombstone", tombstone.Obj.(metav1.Object).GetName())
	}

	ev, err := p.createEvent(object)
	if err != nil {
		klog.V(5).InfoS("failed to create event data", "error", err)
		return
	}

	if err := p.p.Publish(ev, api.EventDelete); err != nil {
		klog.V(5).InfoS("failed to publish event", "error", err)
	}
}