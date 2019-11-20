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

package gcs

import (
	"context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/gcp"
	"strings"
)

// constants
const (
	Type      = "gcs"
	UserAgent = "kcc/controller-manager"
)

// RsrcManager - complies with resource manager interface
type RsrcManager struct {
	name    string
	service *storage.Service
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
func (rm *RsrcManager) WithService(s *storage.Service) *RsrcManager {
	rm.service = s
	return rm
}

// Object - GCS object
type Object struct {
	Bucket    *storage.Bucket
	ProjectID string
}

// SetLabels - set labels
func (o *Object) SetLabels(labels map[string]string) {
	o.Bucket.Labels = gcp.CompliantLabelMap(labels)
}

// SetOwnerReferences - return name string
func (o *Object) SetOwnerReferences(refs *metav1.OwnerReference) bool { return false }

// IsSameAs - return name string
func (o *Object) IsSameAs(a interface{}) bool {
	same := false
	e := a.(*Object)
	if e.Bucket.Name == o.Bucket.Name {
		same = true
	}
	return same
}

// GetName - return name string
func (o *Object) GetName() string {
	return "gcs-bucket/" + o.ProjectID + "/" + o.Bucket.Location + "/" + o.Bucket.Name
}

// Observable captures the k8s resource info and selector to fetch child resources
type Observable struct {
	// Labels list of labels
	Labels map[string]string
	// Object
	Obj *storage.Bucket
	// Project
	ProjectID string
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
		observables = append(observables, reconciler.Observable{Type: Type, Obj: Observable{Obj: obj.Bucket, Labels: labels}})

	}
	return observables
}

// SpecDiffers - check if the spec part differs
func (rm *RsrcManager) SpecDiffers(expected, observed *reconciler.Object) bool {
	e := expected.Obj.(*Object).Bucket
	o := observed.Obj.(*Object).Bucket
	return !reflect.DeepEqual(e.Acl, o.Acl) ||
		!reflect.DeepEqual(e.Billing, o.Billing) ||
		!reflect.DeepEqual(e.Cors, o.Cors) ||
		!reflect.DeepEqual(e.DefaultEventBasedHold, o.DefaultEventBasedHold) ||
		!reflect.DeepEqual(e.Encryption, o.Encryption) ||
		!reflect.DeepEqual(e.Labels, o.Labels) ||
		!reflect.DeepEqual(e.Lifecycle, o.Lifecycle) ||
		!strings.EqualFold(e.Location, o.Location) ||
		!reflect.DeepEqual(e.Logging, o.Logging) ||
		!reflect.DeepEqual(e.Name, o.Name) ||
		!reflect.DeepEqual(e.Owner, o.Owner) ||
		!reflect.DeepEqual(e.StorageClass, o.StorageClass) ||
		!reflect.DeepEqual(e.Versioning, o.Versioning) ||
		!reflect.DeepEqual(e.Website, o.Website)
}

// Observe - get resources
func (rm *RsrcManager) Observe(observables ...reconciler.Observable) ([]reconciler.Object, error) {
	var returnval []reconciler.Object
	for _, item := range observables {
		obs, ok := item.Obj.(Observable)
		if !ok {
			continue
		}
		bkt, err := rm.service.Buckets.Get(obs.Obj.Name).Do()
		if err != nil {
			if gcp.IsNotFound(err) {
				continue
			}
			return []reconciler.Object{}, err
		}
		obj := Object{Bucket: bkt, ProjectID: obs.ProjectID}
		returnval = append(returnval, *obj.AsReconcilerObject())
	}
	return returnval, nil
}

// Update - Generic client update
func (rm *RsrcManager) Update(item reconciler.Object) error {
	bkt := item.Obj.(*Object).Bucket
	_, err := rm.service.Buckets.Patch(bkt.Name, bkt).Do()
	return err
}

// Create - Generic client create
func (rm *RsrcManager) Create(item reconciler.Object) error {
	o := item.Obj.(*Object)
	_, err := rm.service.Buckets.Insert(o.ProjectID, o.Bucket).Do()
	return err
}

// Delete - Generic client delete
func (rm *RsrcManager) Delete(item reconciler.Object) error {
	bkt := item.Obj.(*Object).Bucket
	err := rm.service.Buckets.Delete(bkt.Name).Do()
	return err
}

// NewObject return a new object
func NewObject(name string) (*reconciler.Object, error) {
	project, err := gcp.GetProjectFromMetadata()
	if err != nil {
		return nil, err
	}
	obj := &Object{
		Bucket: &storage.Bucket{
			Name: name,
		},
		ProjectID: project,
	}
	return obj.AsReconcilerObject(), nil
}

// NewService returns a new client
func NewService(ctx context.Context) (*storage.Service, error) {
	httpClient, err := google.DefaultClient(ctx, storage.CloudPlatformScope)
	if err != nil {
		return nil, err
	}
	client, err := storage.New(httpClient)
	if err != nil {
		return nil, err
	}
	client.UserAgent = UserAgent
	return client, nil
}
