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

package genericreconciler

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"reflect"
	"sigs.k8s.io/controller-reconciler/pkg/finalizer"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager"
	"strings"
	"time"
)

// Constants defining labels
const (
	LabelResource          = "custom-resource"
	LabelResourceName      = "custom-resource-name"
	LabelResourceNamespace = "custom-resource-namespace"
	LabelUsing             = "using"
)

// Handler is an interface for operating on logical Components of a resource
type Handler interface {
	Objects(api interface{}, labels map[string]string, observed, dependent, aggregated []reconciler.Object) ([]reconciler.Object, error)
	Observables(api interface{}, labels map[string]string, dependent []reconciler.Object) []reconciler.Observable
}

// StatusInterface - interface to update compoennt status
type StatusInterface interface {
	UpdateStatus(api interface{}, reconciled []reconciler.Object, err error) time.Duration
}

// FinalizeInterface - finalize component
type FinalizeInterface interface {
	Finalize(api interface{}, dependent, observed []reconciler.Object) error
}

// DiffersInterface - call differs
type DiffersInterface interface {
	Differs(expected reconciler.Object, observed reconciler.Object) bool
}

// DependentResourcesInterface - get dependent resources
type DependentResourcesInterface interface {
	DependentResources(api interface{}) []reconciler.Object
}

// getLabels return
func getLabels(ro runtime.Object, using string) map[string]string {
	r := ro.(metav1.Object)
	return map[string]string{
		LabelResource:          strings.Trim(reflect.TypeOf(r).String(), "*"),
		LabelResourceName:      r.GetName(),
		LabelResourceNamespace: r.GetNamespace(),
		LabelUsing:             strings.Trim(using, "*"),
	}
}

// dependentResources Get dependent resources from component or defaults
func dependentResources(h Handler, resource runtime.Object) []reconciler.Object {
	if s, ok := h.(DependentResourcesInterface); ok {
		return s.DependentResources(resource)
	}
	return []reconciler.Object{}
}

// differs - call differs
func differs(h Handler, expected reconciler.Object, observed reconciler.Object) bool {
	if s, ok := h.(DiffersInterface); ok {
		return s.Differs(expected, observed)
	}
	return true
}

// finalize - Finalize component
func finalize(h Handler, resource runtime.Object, observed, dependent []reconciler.Object) error {
	if s, ok := h.(FinalizeInterface); ok {
		return s.Finalize(resource, observed, dependent)
	}
	r := resource.(metav1.Object)
	finalizer.RemoveStandard(r)
	return nil
}

// updateStatus - update component status
func updateStatus(h Handler, resource runtime.Object, reconciled []reconciler.Object, err error) time.Duration {
	var period time.Duration
	if s, ok := h.(StatusInterface); ok {
		return s.UpdateStatus(resource, reconciled, err)
	}
	return period
}

// getAllObservables - get all observables
func getAllObservables(rsrcmgr *manager.ResourceManager, bag []reconciler.Object, labels map[string]string) []reconciler.Observable {
	var observables []reconciler.Observable
	for _, m := range rsrcmgr.All() {
		o := m.ObservablesFromObjects(bag, labels)
		observables = append(observables, o...)
	}
	return observables
}
