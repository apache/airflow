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

package k8s

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
	"log"
	"os"
	"reflect"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"text/template"
)

// constants
const (
	Type = "k8s"
)

// RsrcManager - complies with resource manager interface
type RsrcManager struct {
	name   string
	client client.Client
	scheme *runtime.Scheme
}

// FileResource - file, resource
type FileResource struct {
	Path    string
	ObjList metav1.ListInterface
}

// Getter returns nil manager
func Getter(ctx context.Context, c client.Client, v *runtime.Scheme) func() (string, manager.Manager, error) {
	return func() (string, manager.Manager, error) {
		rm := &RsrcManager{}
		rm.WithClient(c).WithName(Type + "Mgr").WithScheme(v)
		return Type, rm, nil
	}
}

// NewRsrcManager returns nil manager
func NewRsrcManager(ctx context.Context, c client.Client, v *runtime.Scheme) *RsrcManager {
	rm := &RsrcManager{}
	rm.WithClient(c).WithName(Type + "Mgr").WithScheme(v)
	return rm
}

// WithName adds name
func (rm *RsrcManager) WithName(v string) *RsrcManager {
	rm.name = v
	return rm
}

// WithClient attaches client
func (rm *RsrcManager) WithClient(v client.Client) *RsrcManager {
	rm.client = v
	return rm
}

// WithScheme adds scheme
func (rm *RsrcManager) WithScheme(v *runtime.Scheme) *RsrcManager {
	rm.scheme = v
	return rm
}

// GetScheme gets scheme
func (rm *RsrcManager) GetScheme() *runtime.Scheme {
	return rm.scheme
}

// Object - K8s object
type Object struct {
	// Obj refers to the resource object  can be: sts, service, secret, pvc, ..
	Obj metav1.Object
	// ObjList refers to the list of resource objects
	ObjList metav1.ListInterface
}

func isReferringSameObject(a, b metav1.OwnerReference) bool {
	aGV, err := schema.ParseGroupVersion(a.APIVersion)
	if err != nil {
		return false
	}
	bGV, err := schema.ParseGroupVersion(b.APIVersion)
	if err != nil {
		return false
	}
	return aGV == bGV && a.Kind == b.Kind && a.Name == b.Name
}

// SetLabels - set labels
func (o *Object) SetLabels(labels map[string]string) {
	o.Obj.SetLabels(labels)
}

// SetOwnerReferences - return name string
func (o *Object) SetOwnerReferences(ref *metav1.OwnerReference) bool {
	if ref == nil {
		return false
	}
	objRefs := o.Obj.GetOwnerReferences()
	for _, r := range objRefs {
		if isReferringSameObject(*ref, r) {
			return false
		}
	}
	objRefs = append(objRefs, *ref)
	o.Obj.SetOwnerReferences(objRefs)
	return true
}

// IsSameAs - return name string
func (o *Object) IsSameAs(a interface{}) bool {
	same := false
	e := a.(*Object)
	oNamespace := o.Obj.GetNamespace()
	oName := o.Obj.GetName()
	oKind := reflect.TypeOf(o.Obj).String()
	if (e.Obj.GetName() == oName) &&
		(e.Obj.GetNamespace() == oNamespace) &&
		(reflect.TypeOf(e.Obj).String() == oKind) {
		same = true
	}
	return same
}

// GetName - return name string
func (o *Object) GetName() string {
	eNamespace := o.Obj.GetNamespace()
	eName := o.Obj.GetName()
	eKind := reflect.TypeOf(o.Obj).String()
	return eNamespace + "/" + eKind + "/" + eName
}

// Observable captures the k8s resource info and selector to fetch child resources
type Observable struct {
	// ObjList refers to the list of resource objects
	ObjList metav1.ListInterface
	// Obj refers to the resource object  can be: sts, service, secret, pvc, ..
	Obj metav1.Object
	// Labels list of labels
	Labels map[string]string
	// Typemeta - needed for go test fake client
	Type metav1.TypeMeta
}

// LocalObjectReference with validation
type LocalObjectReference struct {
	corev1.LocalObjectReference `json:",inline"`
}

// AsReconcilerObject wraps object as resource item
func (o *Object) AsReconcilerObject() *reconciler.Object {
	return &reconciler.Object{
		Obj:       o,
		Lifecycle: reconciler.LifecycleManaged,
		Type:      Type,
	}
}

// itemFromReader reads Object from []byte spec
func itemFromReader(name string, b *bufio.Reader, data interface{}, list metav1.ListInterface) (*reconciler.Object, error) {
	var exdoc bytes.Buffer
	r := yaml.NewYAMLReader(b)
	doc, err := r.Read()
	if err == nil {
		tmpl, e := template.New("tmpl").Parse(string(doc))
		err = e
		if err == nil {
			err = tmpl.Execute(&exdoc, data)
			if err == nil {
				d := scheme.Codecs.UniversalDeserializer()
				obj, _, e := d.Decode(exdoc.Bytes(), nil, nil)
				err = e
				if err == nil {
					return &reconciler.Object{
						Type:      Type,
						Lifecycle: reconciler.LifecycleManaged,
						Obj: &Object{
							Obj:     obj.DeepCopyObject().(metav1.Object),
							ObjList: list,
						},
					}, nil
				}
				log.Printf("   >>>ERR decoding : %s\n", exdoc.String())
			}
		}
	}

	return nil, errors.New(name + ":" + err.Error())
}

// ObjectFromString populates Object from string spec
func ObjectFromString(name, spec string, values interface{}, list metav1.ListInterface) (*reconciler.Object, error) {
	return itemFromReader(name, bufio.NewReader(strings.NewReader(spec)), values, list)
}

// ObjectFromFile populates Object from file
func ObjectFromFile(path string, values interface{}, list metav1.ListInterface) (*reconciler.Object, error) {
	f, err := os.Open(path)
	if err == nil {
		return itemFromReader(path, bufio.NewReader(f), values, list)
	}
	return nil, err
}

// ObjectsFromFiles populates Object from file
func ObjectsFromFiles(values interface{}, fileResources []FileResource) ([]reconciler.Object, error) {
	items := []reconciler.Object{}
	for _, fr := range fileResources {
		o, err := ObjectFromFile(fr.Path, values, fr.ObjList)
		if err != nil {
			return nil, err
		}
		items = append(items, *o)
	}
	return items, nil
}

// NewObservable returns an observable object
func NewObservable(list metav1.ListInterface, labels map[string]string) reconciler.Observable {
	return reconciler.Observable{
		Type: Type,
		Obj: Observable{
			ObjList: list,
			Labels:  labels,
		},
	}
}

// ObservablesFromObjects returns ObservablesFromObjects
func (rm *RsrcManager) ObservablesFromObjects(bag []reconciler.Object, labels map[string]string) []reconciler.Observable {
	var gk schema.GroupKind
	var observables []reconciler.Observable
	gkmap := map[schema.GroupKind]struct{}{}
	for _, item := range bag {
		if item.Type != Type {
			continue
		}
		obj, ok := item.Obj.(*Object)
		if !ok {
			continue
		}
		if obj.ObjList != nil {
			ro := obj.Obj.(runtime.Object)
			kinds, _, err := rm.scheme.ObjectKinds(ro)
			if err == nil {
				// Expect only 1 kind.  If there is more than one kind this is probably an edge case such as ListOptions.
				if len(kinds) != 1 {
					err = fmt.Errorf("Expected exactly 1 kind for Object %T, but found %s kinds", ro, kinds)

				}
			}
			// Cache the Group and Kind for the OwnerType
			if err == nil {
				gk = schema.GroupKind{Group: kinds[0].Group, Kind: kinds[0].Kind}
			} else {
				gk = ro.GetObjectKind().GroupVersionKind().GroupKind()
			}
			if _, ok := gkmap[gk]; !ok {
				gkmap[gk] = struct{}{}
				observable := reconciler.Observable{
					Type: Type,
					Obj: Observable{
						ObjList: obj.ObjList,
						Labels:  labels,
					},
				}
				observables = append(observables, observable)
			}
		} else {
			observable := reconciler.Observable{
				Type: Type,
				Obj: Observable{
					Obj: obj.Obj,
				},
			}
			observables = append(observables, observable)
		}
	}
	return observables
}

// Validate validates the LocalObjectReference
func (s *LocalObjectReference) Validate(fp *field.Path, sfield string, errs field.ErrorList, required bool) field.ErrorList {
	fp = fp.Child(sfield)
	if s == nil {
		if required {
			errs = append(errs, field.Required(fp, "Required "+sfield+" missing"))
		}
		return errs
	}

	if s.Name == "" {
		errs = append(errs, field.Required(fp.Child("name"), "name is required"))
	}
	return errs
}

// FilterObservable - remove from observable
func FilterObservable(observable []reconciler.Observable, list metav1.ListInterface) []reconciler.Observable {
	var filtered []reconciler.Observable
	ltype := reflect.TypeOf(list).String()
	for i := range observable {
		otype := ""
		o, ok := observable[i].Obj.(Observable)
		if ok {
			otype = reflect.TypeOf(o.ObjList).String()
		}
		if ltype != otype {
			filtered = append(filtered, observable[i])
		}
	}
	return filtered

}

// ReferredItem returns a reffered object
func ReferredItem(obj metav1.Object, name, namespace string) reconciler.Object {
	obj.SetName(name)
	obj.SetNamespace(namespace)
	return reconciler.Object{
		Lifecycle: reconciler.LifecycleReferred,
		Type:      Type,
		Obj:       &Object{Obj: obj},
	}
}

// GetItem returns an item which matched the kind and name
func GetItem(b []reconciler.Object, inobj metav1.Object, name, namespace string) metav1.Object {
	inobj.SetName(name)
	inobj.SetNamespace(namespace)
	for _, item := range reconciler.ObjectsByType(b, Type) {
		obj := item.Obj.(*Object)
		otype := reflect.TypeOf(obj.Obj).String()
		intype := reflect.TypeOf(inobj).String()
		if otype == intype && obj.Obj.GetName() == inobj.GetName() && obj.Obj.GetNamespace() == inobj.GetNamespace() {
			return obj.Obj
		}
	}
	return nil
}

/* TODO
// Remove returns an item which matched the kind and name
func Remove(b []reconciler.Object, inobj metav1.Object) {
	for i, item := range reconciler.ObjectsByType(b, Type) {
		if item.Type == Type {
			obj := item.Obj.(*Object)
			otype := reflect.TypeOf(obj.Obj).String()
			intype := reflect.TypeOf(inobj).String()
			if otype == intype && obj.Obj.GetName() == inobj.GetName() && obj.Obj.GetNamespace() == inobj.GetNamespace() {
				b.DeleteAt(i)
				break
			}
		}
	}
}
*/

// IsSameKind - true if same kind
func IsSameKind(o *reconciler.Object, inobj metav1.Object) bool {
	same := false
	if o.Type == Type {
		obj := o.Obj.(*Object)
		okind := reflect.TypeOf(obj.Obj).String()
		inkind := reflect.TypeOf(inobj).String()
		if okind == inkind {
			same = true
		}
	}
	return same
}

// Objs get items from the Object bag
func Objs(b []reconciler.Object) []metav1.Object {
	var objs []metav1.Object
	for _, item := range reconciler.ObjectsByType(b, Type) {
		o := item.Obj.(*Object)
		objs = append(objs, o.Obj)
	}
	return objs
}

// CopyMutatedSpecFields - copy known mutated fields from observed to expected
func CopyMutatedSpecFields(to *reconciler.Object, from *reconciler.Object) {
	e := to.Obj.(*Object)
	o := from.Obj.(*Object)
	e.Obj.SetOwnerReferences(o.Obj.GetOwnerReferences())
	e.Obj.SetResourceVersion(o.Obj.GetResourceVersion())
	// TODO
	switch e.Obj.(type) {
	case *corev1.Service:
		e.Obj.(*corev1.Service).Spec.ClusterIP = o.Obj.(*corev1.Service).Spec.ClusterIP
	case *policyv1.PodDisruptionBudget:
		//e.Obj.SetResourceVersion(o.Obj.GetResourceVersion())
	case *corev1.PersistentVolumeClaim:
		e.Obj.(*corev1.PersistentVolumeClaim).Spec.StorageClassName = o.Obj.(*corev1.PersistentVolumeClaim).Spec.StorageClassName
	}
}

// SpecDiffers - check if the spec part differs
func (rm *RsrcManager) SpecDiffers(expected, observed *reconciler.Object) bool {
	e := expected.Obj.(*Object)
	o := observed.Obj.(*Object)

	// Not all k8s objects have Spec
	// example ConfigMap
	// TODO strategic merge patch diff in generic controller loop

	CopyMutatedSpecFields(expected, observed)

	espec := reflect.Indirect(reflect.ValueOf(e.Obj)).FieldByName("Spec")
	ospec := reflect.Indirect(reflect.ValueOf(o.Obj)).FieldByName("Spec")
	if !espec.IsValid() {
		// handling ConfigMap
		espec = reflect.Indirect(reflect.ValueOf(e.Obj)).FieldByName("Data")
		ospec = reflect.Indirect(reflect.ValueOf(o.Obj)).FieldByName("Data")
	}
	if espec.IsValid() && ospec.IsValid() {
		if reflect.DeepEqual(espec.Interface(), ospec.Interface()) {
			return false
		}
	}
	return true
}

// Observe - get resources
func (rm *RsrcManager) Observe(observables ...reconciler.Observable) ([]reconciler.Object, error) {
	var returnval []reconciler.Object
	var err error
	for _, item := range observables {
		var resources []reconciler.Object
		obs, ok := item.Obj.(Observable)
		if !ok {
			continue
		}
		if obs.Labels != nil {
			//log.Printf("   >>>list: %s labels:[%v]", reflect.TypeOf(obs.ObjList).String(), obs.Labels)
			opts := client.MatchingLabels(obs.Labels)
			opts.Raw = &metav1.ListOptions{TypeMeta: obs.Type}
			err = rm.client.List(context.TODO(), opts, obs.ObjList.(runtime.Object))
			if err == nil {
				items, err := meta.ExtractList(obs.ObjList.(runtime.Object))
				if err == nil {
					for _, item := range items {
						resources = append(resources, reconciler.Object{Type: Type, Obj: &Object{Obj: item.(metav1.Object)}})
					}
				}
				/*
					//items := reflect.Indirect(reflect.ValueOf(obs.ObjList)).FieldByName("Items")
					for i := 0; i < items.Len(); i++ {
						o := items.Index(i)
						resources = append(resources, object.Object{Obj: o.Addr().Interface().(metav1.Object)})
					}
				*/
			}
		} else {
			// check typecasting ?
			// TODO check obj := obs.Obj.(metav1.Object)
			var obj metav1.Object = obs.Obj.(metav1.Object)
			name := obj.GetName()
			namespace := obj.GetNamespace()
			otype := reflect.TypeOf(obj).String()
			err = rm.client.Get(context.TODO(),
				types.NamespacedName{Name: name, Namespace: namespace},
				obs.Obj.(runtime.Object))
			if err == nil {
				log.Printf("   >>get: %s", otype+"/"+namespace+"/"+name)
				resources = append(resources, reconciler.Object{Type: Type, Obj: &Object{Obj: obs.Obj}})
			} else {
				log.Printf("   >>>ERR get: %s", otype+"/"+namespace+"/"+name)
			}
		}
		if err != nil {
			return nil, err
		}
		for _, resource := range resources {
			returnval = append(returnval, resource)
		}
	}
	return returnval, nil
}

// Update - Generic client update
func (rm *RsrcManager) Update(item reconciler.Object) error {
	return rm.client.Update(context.TODO(), item.Obj.(*Object).Obj.(runtime.Object).DeepCopyObject())

}

// Create - Generic client create
func (rm *RsrcManager) Create(item reconciler.Object) error {
	return rm.client.Create(context.TODO(), item.Obj.(*Object).Obj.(runtime.Object))
}

// Delete - Generic client delete
func (rm *RsrcManager) Delete(item reconciler.Object) error {
	return rm.client.Delete(context.TODO(), item.Obj.(*Object).Obj.(runtime.Object))
}

// Get a specific k8s obj
func Get(rm manager.Manager, nn types.NamespacedName, o runtime.Object) error {
	krm := rm.(*RsrcManager)
	return krm.client.Get(context.TODO(), nn, o)
}

//---------------------- Objects ---------------------------------------

// Objects internal
type Objects struct {
	bag    []reconciler.Object
	folder string
	err    error
	value  interface{}
}

// NewObjects returns nag
func NewObjects() *Objects {
	return &Objects{
		bag:    []reconciler.Object{},
		folder: "templates/",
		err:    nil,
	}
}

//WithValue injects template value
func (b *Objects) WithValue(value interface{}) *Objects {
	b.value = value
	return b
}

//WithFolder injects folder
func (b *Objects) WithFolder(path string) *Objects {
	b.folder = path
	return b
}

//WithTemplate - add an item from template
func (b *Objects) WithTemplate(file string, list metav1.ListInterface, mutators ...func(*reconciler.Object, interface{})) *Objects {
	item, err := ObjectFromFile(b.folder+file, b.value, list)

	if err == nil {
		for _, fn := range mutators {
			fn(item, b.value)
		}
		b.bag = append(b.bag, *item)
	} else {
		// TODO accumulate vs overwrite
		b.err = err
	}
	return b
}

// WithObject - add a static object
func (b *Objects) WithObject(objfn func(interface{}) metav1.Object, list metav1.ListInterface) *Objects {
	o := &reconciler.Object{
		Lifecycle: reconciler.LifecycleManaged,
		Type:      Type,
		Obj:       &Object{Obj: objfn(b.value), ObjList: list},
	}
	b.bag = append(b.bag, *o)
	return b
}

// WithReferredItem returns a reffered object
func (b *Objects) WithReferredItem(obj metav1.Object, name, namespace string) *Objects {
	obj.SetName(name)
	obj.SetNamespace(namespace)
	o := reconciler.Object{
		Lifecycle: reconciler.LifecycleReferred,
		Type:      Type,
		Obj:       &Object{Obj: obj},
	}
	b.bag = append(b.bag, o)
	return b
}

//Build - process
func (b *Objects) Build() ([]reconciler.Object, error) {
	return b.bag, b.err
}

// --------------------- Observables -------------------------------

// Observables - i
type Observables struct {
	observables []reconciler.Observable
	labels      reconciler.KVMap
}

// NewObservables - observables
func NewObservables() *Observables {
	return &Observables{
		observables: []reconciler.Observable{},
	}
}

// WithLabels - inject labels
func (o *Observables) WithLabels(labels reconciler.KVMap) *Observables {
	o.labels = labels
	return o
}

// For - add
func (o *Observables) For(list metav1.ListInterface) *Observables {
	o.observables = append(o.observables, NewObservable(list, o.labels))
	return o
}

// Add - add
func (o *Observables) Add(obs reconciler.Observable) *Observables {
	o.observables = append(o.observables, obs)
	return o
}

// Get - return observable array
func (o *Observables) Get() []reconciler.Observable {
	return o.observables
}
