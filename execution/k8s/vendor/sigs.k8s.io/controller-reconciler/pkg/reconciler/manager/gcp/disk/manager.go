/*
Copyright 2018 Google LLC
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

package disk

import (
	"context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/gcp"
)

// constants
const (
	Type      = "disk"
	UserAgent = "kcc/controller-manager"
)

// RsrcManager - complies with resource manager interface
type RsrcManager struct {
	name    string
	service *compute.Service
}

// Getter returns nil manager
func Getter(ctx context.Context) func() (string, manager.Manager, error) {
	return func() (string, manager.Manager, error) {
		rm := &RsrcManager{}
		service, err := NewService(ctx)
		if err != nil {
			return "", nil, err
		}
		rm.WithService(service).WithName(Type + "Mgr")
		return Type, rm, nil
	}
}

// NewRsrcManager returns nil manager
func NewRsrcManager(ctx context.Context, name string) (*RsrcManager, error) {
	rm := &RsrcManager{}
	service, err := NewService(ctx)
	if err != nil {
		return nil, err
	}
	rm.WithService(service).WithName(name)
	return rm, nil
}

// WithName adds name
func (rm *RsrcManager) WithName(v string) *RsrcManager {
	rm.name = v
	return rm
}

// WithService adds storage service
func (rm *RsrcManager) WithService(s *compute.Service) *RsrcManager {
	rm.service = s
	return rm
}

// Object - PD object
type Object struct {
	Disk    *compute.Disk
	Project string
}

// SetOwnerReferences - return name string
func (o *Object) SetOwnerReferences(refs *metav1.OwnerReference) bool { return false }

// SetLabels - set labels
func (o *Object) SetLabels(labels map[string]string) {
	o.Disk.Labels = gcp.CompliantLabelMap(labels)
}

// IsSameAs - return name string
func (o *Object) IsSameAs(a interface{}) bool {
	same := false
	e := a.(*Object)
	if e.Disk.Name == o.Disk.Name {
		same = true
	}
	return same
}

// GetName - return name string
func (o *Object) GetName() string {
	return "pd/" + o.Project + "/" + o.Disk.Zone + "/" + o.Disk.Name
}

// Observable captures the k8s resource info and selector to fetch child resources
type Observable struct {
	// Labels list of labels
	Labels map[string]string
	// Object
	Obj *compute.Disk
	// Project
	Project string
}

// AsReconcilerObject wraps object as resource item
func (o *Object) AsReconcilerObject() *reconciler.Object {
	return &reconciler.Object{
		Obj:       o,
		Lifecycle: reconciler.LifecycleManaged,
		Type:      Type,
	}
}

// NewObservable returns an observable object
func NewObservable(labels map[string]string) reconciler.Observable {
	return reconciler.Observable{
		Type: Type,
		Obj: Observable{
			Labels: labels,
		},
	}
}

// ObservablesFromObjects returns ObservablesFromObjects
func (rm *RsrcManager) ObservablesFromObjects(bag []reconciler.Object, labels map[string]string) []reconciler.Observable {
	var observables []reconciler.Observable
	for _, item := range bag {
		if item.Type != Type {
			continue
		}
		obj, ok := item.Obj.(*Object)
		if !ok {
			continue
		}
		observables = append(observables, reconciler.Observable{Type: Type, Obj: Observable{Obj: obj.Disk, Labels: labels}})

	}
	return observables
}

// SpecDiffers - check if the spec part differs
func (rm *RsrcManager) SpecDiffers(expected, observed *reconciler.Object) bool {
	e := expected.Obj.(*Object).Disk
	o := observed.Obj.(*Object).Disk
	// TODO
	return !reflect.DeepEqual(e.Labels, o.Labels) ||
		!reflect.DeepEqual(e.Name, o.Name) ||
		!reflect.DeepEqual(e.SizeGb, o.SizeGb)
}

// Observe - get resources
func (rm *RsrcManager) Observe(observables ...reconciler.Observable) ([]reconciler.Object, error) {
	var returnval []reconciler.Object
	project, err := gcp.GetProjectFromMetadata()
	if err != nil {
		return nil, err
	}
	zone, err := gcp.GetZoneFromMetadata()
	if err != nil {
		return nil, err
	}
	for _, item := range observables {
		obs, ok := item.Obj.(Observable)
		if !ok {
			continue
		}
		disklist, err := rm.service.Disks.List(project, zone).
			Filter(gcp.GetFilterStringFromLabels(obs.Labels)).
			Do()
		if err != nil {
			return []reconciler.Object{}, err
		}
		for _, disk := range disklist.Items {
			obj := Object{Disk: disk}
			returnval = append(returnval, *obj.AsReconcilerObject())
		}
		/*
			d := obs.Obj
			disk, err := rm.service.Disks.Get(obs.Project, d.Zone, d.Name).Do()
			if err != nil {
				return []reconciler.Object{}, err
			}
			obj := Object{Obj: disk}
		*/
	}
	return returnval, nil
}

// Update - Generic client update
func (rm *RsrcManager) Update(item reconciler.Object) error {
	//obj := item.Obj.(*Object)
	//d := obj.Obj
	//size := compute.DisksResizeRequest{SizeGb: d.SizeGb}
	//_, err := rm.service.Disks.Resize(obj.Project, d.Zone, d.Name, &size).Do()
	//return err
	return nil
}

// Create - Generic client create
func (rm *RsrcManager) Create(item reconciler.Object) error {
	obj := item.Obj.(*Object)
	d := obj.Disk
	_, err := rm.service.Disks.Insert(obj.Project, d.Zone, d).Do()
	return err
}

// Delete - Generic client delete
func (rm *RsrcManager) Delete(item reconciler.Object) error {
	obj := item.Obj.(*Object)
	d := obj.Disk
	_, err := rm.service.Disks.Delete(obj.Project, d.Zone, d.Name).Do()
	return err
}

// NewObject return a new object
func NewObject(name string, size int64) (*reconciler.Object, error) {
	project, err := gcp.GetProjectFromMetadata()
	if err != nil {
		return nil, err
	}
	zone, err := gcp.GetZoneFromMetadata()
	if err != nil {
		return nil, err
	}
	obj := &Object{
		Disk: &compute.Disk{
			Name:   name,
			Zone:   zone,
			SizeGb: size,
		},
		Project: project,
	}
	return obj.AsReconcilerObject(), nil
}

// NewService returns a new client
func NewService(ctx context.Context) (*compute.Service, error) {
	httpClient, err := google.DefaultClient(ctx, compute.CloudPlatformScope)
	if err != nil {
		return nil, err
	}
	client, err := compute.New(httpClient)
	if err != nil {
		return nil, err
	}
	client.UserAgent = UserAgent
	return client, nil
}
