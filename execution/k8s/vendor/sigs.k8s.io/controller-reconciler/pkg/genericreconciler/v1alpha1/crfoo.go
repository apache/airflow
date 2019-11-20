/*
Copyright 2018 The Kubernetes Authors.

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

package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"log"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler"
	"sigs.k8s.io/controller-reconciler/pkg/reconciler/manager/k8s"
	crscheme "sigs.k8s.io/controller-runtime/pkg/runtime/scheme"
)

var (
	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder = &crscheme.Builder{
		GroupVersion: schema.GroupVersion{
			Group:   "foo.cloud.google.com",
			Version: "v1alpha1",
		},
	}
	// SchemeGroupVersion - GV
	SchemeGroupVersion = schema.GroupVersion{
		Group:   "foo.cloud.google.com",
		Version: "v1alpha1",
	}
)

// Foo test custom resource
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Foo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              FooSpec   `json:"spec,omitempty"`
	Status            FooStatus `json:"status,omitempty"`
}

// FooSpec CR foo spec
type FooSpec struct {
	Version string
}

// FooList contains a list of Foo
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type FooList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Foo `json:"items"`
}

// FooStatus CR foo status
type FooStatus struct {
	Status    string
	Component string
}

// FooHandler handler
type FooHandler struct{}

// Objects - returns resources
func (s *FooHandler) Objects(rsrc interface{}, rsrclabels map[string]string, observed, dependent, aggregate []reconciler.Object) ([]reconciler.Object, error) {
	var resources []reconciler.Object
	r := rsrc.(*Foo)
	n := r.ObjectMeta.Name
	ns := r.ObjectMeta.Namespace
	resources = append(resources,
		[]reconciler.Object{
			{
				Lifecycle: reconciler.LifecycleManaged,
				Type:      k8s.Type,
				Obj: &k8s.Object{
					ObjList: &appsv1.DeploymentList{},
					Obj: &appsv1.Deployment{
						ObjectMeta: metav1.ObjectMeta{
							Name:      n + "-deploy",
							Namespace: ns,
							Labels:    rsrclabels,
						},
					},
				},
			},
			{
				Lifecycle: reconciler.LifecycleManaged,
				Type:      k8s.Type,
				Obj: &k8s.Object{
					ObjList: &corev1.ConfigMapList{},
					Obj: &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      n + "-cm",
							Namespace: ns,
							Labels:    rsrclabels,
						},
						Data: map[string]string{
							"test-key": "test-value",
						},
					},
				},
			},
		}...,
	)
	return resources, nil
}

// Observables - return selectors
func (s *FooHandler) Observables(rsrc interface{}, rsrclabels map[string]string, dependent []reconciler.Object) []reconciler.Observable {
	return []reconciler.Observable{
		{
			Type: k8s.Type,
			Obj: &k8s.Observable{
				ObjList: &appsv1.DeploymentList{},
				Labels:  rsrclabels,
				Type:    metav1.TypeMeta{Kind: "Deployment", APIVersion: "apps/v1"},
			},
		},
		{
			Type: k8s.Type,
			Obj: &k8s.Observable{
				ObjList: &corev1.ConfigMapList{},
				Labels:  rsrclabels,
				Type:    metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
			},
		},
	}
}

func init() {
	SchemeBuilder.Register(&Foo{}, &FooList{})
	err := SchemeBuilder.AddToScheme(scheme.Scheme)
	if err != nil {
		log.Fatal(err)
	}
}
